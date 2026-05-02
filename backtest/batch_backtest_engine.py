#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
增量批处理回测引擎

功能:
1. 分批次处理回测，每批只加载当前需要的数据
2. 处理完一批立即释放内存，避免内存溢出
3. 支持断点续传，从上次中断位置继续
4. 合并各批次结果生成完整回测报告
"""

import os
import json
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field, asdict
import gc

import pandas as pd
import numpy as np

from backtest.local_data_loader import LocalDataLoader
from backtest.signal_calculator import SignalCalculator, SignalConfig
from backtest.streaming_strategy import (
    SignalLookupManager,
    StreamingStrategyConfig,
)

logger = logging.getLogger(__name__)


@dataclass
class BatchBacktestConfig:
    """批次回测配置"""
    initial_capital: float = 100000.0
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_tax_rate: float = 0.001

    batch_duration_days: int = 30
    overlap_days: int = 5

    data_dir: str = None
    signal_dir: str = None

    output_dir: str = './backtest_results'
    save_intermediate: bool = True

    resume_from: str = None

    def __post_init__(self):
        if self.output_dir:
            Path(self.output_dir).mkdir(parents=True, exist_ok=True)


@dataclass
class BatchState:
    """批次状态 - 用于批次间传递"""
    batch_idx: int
    end_date: str

    positions: Dict[str, Dict] = field(default_factory=dict)
    cash: float = 0.0
    portfolio_value: float = 0.0

    total_trades: int = 0
    total_commission: float = 0.0

    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict) -> 'BatchState':
        positions = data.get('positions', {})
        for symbol, pos in positions.items():
            buy_date = pos.get('buy_date')
            if isinstance(buy_date, str):
                pos['buy_date'] = datetime.strptime(buy_date, '%Y-%m-%d')
        return cls(**data)

    def save(self, filepath: str):
        with open(filepath, 'wb') as f:
            pickle.dump(self.to_dict(), f)

    @classmethod
    def load(cls, filepath: str) -> 'BatchState':
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        return cls.from_dict(data)


@dataclass
class SingleBatchResult:
    """单个批次的回测结果"""
    batch_idx: int
    start_date: str
    end_date: str

    total_return: float = 0.0
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0

    total_trades: int = 0
    win_trades: int = 0
    loss_trades: int = 0

    final_value: float = 0.0

    daily_returns: List[Dict] = field(default_factory=list)
    trades: List[Dict] = field(default_factory=list)

    def to_dict(self) -> Dict:
        return asdict(self)


class IncrementalBacktestEngine:
    """增量批处理回测引擎"""

    def __init__(
        self,
        config: BatchBacktestConfig,
        strategy_config: StreamingStrategyConfig = None,
    ):
        self.config = config
        self.strategy_config = strategy_config or StreamingStrategyConfig()

        self.data_loader = LocalDataLoader(data_dir=config.data_dir)

        self.output_dir = Path(config.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.batch_results: List[SingleBatchResult] = []
        self.final_state: Optional[BatchState] = None

        logger.info(f"增量回测引擎初始化完成")
        logger.info(f"批次天数: {config.batch_duration_days}")

    def _split_date_range(self, start_date: str, end_date: str) -> List[Tuple[str, str]]:
        """将日期范围分割成多个批次"""
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        batches = []
        current_start = start

        while current_start < end:
            batch_end = min(
                current_start + timedelta(days=self.config.batch_duration_days - 1),
                end
            )

            batches.append((
                current_start.strftime('%Y%m%d'),
                batch_end.strftime('%Y%m%d')
            ))

            current_start = batch_end - timedelta(days=self.config.overlap_days - 1)
            if current_start <= batch_end:
                current_start = batch_end + timedelta(days=1)

        logger.info(f"日期范围分割为 {len(batches)} 个批次")
        return batches

    def _execute_backtest(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str],
        initial_capital: float,
        prev_positions: Dict[str, Dict] = None,
    ) -> Dict:
        """执行逐日回测"""
        logger.info(f"执行回测: {start_date} ~ {end_date}, 股票数: {len(symbols)}")

        cash = initial_capital
        positions: Dict[str, Dict] = {}

        if prev_positions:
            positions = {k: v.copy() for k, v in prev_positions.items()}
            for pos in positions.values():
                buy_date = pos.get('buy_date')
                if isinstance(buy_date, str):
                    pos['buy_date'] = datetime.strptime(buy_date, '%Y-%m-%d')
            tied_up_capital = sum(
                pos.get('quantity', 0) * pos.get('buy_price', 0)
                for pos in positions.values()
            )
            cash = initial_capital - tied_up_capital

        trades: List[Dict] = []
        daily_returns: List[Dict] = []
        portfolio_values: List[float] = [initial_capital]

        all_data = []
        for symbol in symbols:
            try:
                df = self.data_loader.load_single(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    df['symbol'] = symbol
                    all_data.append(df)
            except Exception as e:
                logger.debug(f"加载 {symbol} 数据失败: {e}")

        if not all_data:
            return {
                'total_return': 0.0,
                'final_value': initial_capital,
                'trades': [],
                'positions': {},
                'cash': initial_capital,
                'daily_returns': [],
            }

        combined_df = pd.concat(all_data)
        if not isinstance(combined_df.index, pd.DatetimeIndex):
            combined_df.index = pd.to_datetime(combined_df.index)

        trading_days = sorted(combined_df.index.unique())

        signal_manager = SignalLookupManager(
            signal_dir=self.config.signal_dir or str(self.data_loader.data_dir / 'signals'),
            start_date=start_date,
            end_date=end_date,
        )

        config = self.strategy_config

        for i, current_date in enumerate(trading_days):
            date_str = current_date.strftime('%Y-%m-%d')
            day_data = combined_df[combined_df.index == current_date]

            try:
                buy_signals = signal_manager.get_signals_for_date(
                    current_date, config.buy_signal_type, config.min_signal_strength
                )
            except Exception:
                buy_signals = []

            # 处理卖出
            symbols_to_sell = []
            for symbol, pos in list(positions.items()):
                symbol_data = day_data[day_data['symbol'] == symbol]
                if symbol_data.empty:
                    continue
                current_price = symbol_data['close'].iloc[0]

                pnl_pct = (current_price - pos['buy_price']) / pos['buy_price']
                if pnl_pct <= config.stop_loss_pct:
                    symbols_to_sell.append((symbol, 'stop_loss'))
                elif pnl_pct >= config.stop_profit_pct:
                    symbols_to_sell.append((symbol, 'stop_profit'))
                elif (current_date - pos['buy_date']).days >= config.max_hold_days:
                    symbols_to_sell.append((symbol, 'max_hold'))

            for symbol, reason in symbols_to_sell:
                if symbol not in positions:
                    continue

                symbol_data = day_data[day_data['symbol'] == symbol]
                if symbol_data.empty:
                    continue

                sell_price = symbol_data['open'].iloc[0] if config.sell_time == 'open' else symbol_data['close'].iloc[0]
                pos = positions[symbol]
                quantity = pos['quantity']

                sell_value = sell_price * quantity
                commission = max(sell_value * config.commission_rate, config.min_commission)
                stamp_tax = sell_value * config.stamp_tax_rate
                net_value = sell_value - commission - stamp_tax

                cash += net_value

                buy_value = pos['buy_price'] * quantity
                pnl = net_value - buy_value
                pnl_pct = pnl / buy_value * 100

                trades.append({
                    'symbol': symbol,
                    'buy_date': pos['buy_date'].strftime('%Y-%m-%d'),
                    'buy_price': pos['buy_price'],
                    'sell_date': date_str,
                    'sell_price': sell_price,
                    'quantity': quantity,
                    'pnl': pnl,
                    'pnl_pct': pnl_pct,
                    'hold_days': (current_date - pos['buy_date']).days,
                    'sell_reason': reason,
                })

                del positions[symbol]

            # 处理买入
            available_slots = config.max_positions - len(positions)

            if available_slots > 0 and cash > 10000:
                cash_per_slot = cash / available_slots

                for symbol in buy_signals:
                    if symbol in positions or len(positions) >= config.max_positions:
                        continue

                    symbol_data = day_data[day_data['symbol'] == symbol]
                    if symbol_data.empty:
                        continue

                    buy_price = symbol_data['open'].iloc[0] if config.buy_time == 'open' else symbol_data['close'].iloc[0]
                    quantity = int(cash_per_slot / buy_price / 100) * 100

                    if quantity < 100:
                        continue

                    buy_value = buy_price * quantity
                    commission = max(buy_value * config.commission_rate, config.min_commission)

                    if buy_value + commission > cash:
                        continue

                    cash -= (buy_value + commission)
                    positions[symbol] = {
                        'quantity': quantity,
                        'buy_price': buy_price,
                        'buy_date': current_date,
                    }

            # 计算组合价值
            portfolio_value = cash
            for symbol, pos in positions.items():
                symbol_data = day_data[day_data['symbol'] == symbol]
                if not symbol_data.empty:
                    portfolio_value += symbol_data['close'].iloc[0] * pos['quantity']

            portfolio_values.append(portfolio_value)

            if len(portfolio_values) > 1:
                daily_return = (portfolio_values[-1] - portfolio_values[-2]) / portfolio_values[-2]
                daily_returns.append({
                    'date': date_str,
                    'portfolio_value': portfolio_value,
                    'daily_return': daily_return,
                })

        final_value = portfolio_values[-1]
        total_return = (final_value - initial_capital) / initial_capital

        max_drawdown = 0
        peak = initial_capital
        for value in portfolio_values:
            if value > peak:
                peak = value
            drawdown = (peak - value) / peak
            if drawdown > max_drawdown:
                max_drawdown = drawdown

        signal_manager.clear_cache()

        return {
            'total_return': total_return,
            'final_value': final_value,
            'trades': trades,
            'positions': positions,
            'cash': cash,
            'daily_returns': daily_returns,
            'max_drawdown': max_drawdown,
        }

    def run(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None,
    ) -> Dict:
        """运行增量批处理回测"""
        if symbols is None:
            symbols = self.data_loader.available_symbols

        date_batches = self._split_date_range(start_date, end_date)

        start_batch_idx = 0
        prev_state = None

        if self.config.resume_from:
            try:
                prev_state = BatchState.load(self.config.resume_from)
                start_batch_idx = prev_state.batch_idx + 1
            except Exception as e:
                logger.warning(f"加载检查点失败: {e}")

        for batch_idx, (batch_start, batch_end) in enumerate(date_batches):
            if batch_idx < start_batch_idx:
                continue

            batch_result, end_state = self._run_single_batch(
                batch_idx=batch_idx,
                start_date=batch_start,
                end_date=batch_end,
                prev_state=prev_state,
                symbols=symbols,
            )

            self.batch_results.append(batch_result)
            prev_state = end_state

            if self.config.save_intermediate:
                self._save_intermediate_result(batch_result, end_state)

        self.final_state = prev_state

        final_result = self._merge_results()
        self._save_final_result(final_result)

        return final_result

    def _run_single_batch(
        self,
        batch_idx: int,
        start_date: str,
        end_date: str,
        prev_state: Optional[BatchState] = None,
        symbols: List[str] = None,
    ) -> Tuple[SingleBatchResult, BatchState]:
        """运行单个批次的回测"""
        logger.info(f"\n{'='*60}")
        logger.info(f"运行批次 {batch_idx+1}: {start_date} ~ {end_date}")
        logger.info(f"{'='*60}")

        if prev_state:
            positions_value = sum(
                pos.get('quantity', 0) * pos.get('buy_price', 0)
                for pos in prev_state.positions.values()
            )
            initial_capital = prev_state.cash + positions_value
        else:
            initial_capital = self.config.initial_capital

        result = self._execute_backtest(
            start_date=start_date,
            end_date=end_date,
            symbols=symbols,
            initial_capital=initial_capital,
            prev_positions=prev_state.positions if prev_state else None,
        )

        batch_result = SingleBatchResult(
            batch_idx=batch_idx,
            start_date=start_date,
            end_date=end_date,
            total_return=result.get('total_return', 0),
            final_value=result.get('final_value', initial_capital),
            max_drawdown=result.get('max_drawdown', 0),
            total_trades=len(result.get('trades', [])),
            win_trades=sum(1 for t in result.get('trades', []) if t.get('pnl', 0) > 0),
            loss_trades=sum(1 for t in result.get('trades', []) if t.get('pnl', 0) <= 0),
            trades=result.get('trades', []),
            daily_returns=result.get('daily_returns', []),
        )

        end_state = BatchState(
            batch_idx=batch_idx,
            end_date=end_date,
            positions=result.get('positions', {}),
            cash=result.get('cash', initial_capital),
            portfolio_value=result.get('final_value', initial_capital),
        )

        return batch_result, end_state

    def _save_intermediate_result(self, batch_result: SingleBatchResult, state: BatchState):
        """保存中间结果"""
        result_file = self.output_dir / f'batch_{batch_result.batch_idx:04d}_result.json'
        with open(result_file, 'w', encoding='utf-8') as f:
            json.dump(batch_result.to_dict(), f, indent=2, default=str)

        state_file = self.output_dir / f'batch_{batch_result.batch_idx:04d}_state.pkl'
        state.save(str(state_file))

    def _merge_results(self) -> Dict:
        """合并所有批次的回测结果"""
        if not self.batch_results:
            return {}

        all_trades = []
        for result in self.batch_results:
            all_trades.extend(result.trades)

        all_daily_returns = []
        for result in self.batch_results:
            all_daily_returns.extend(result.daily_returns)

        all_daily_returns.sort(key=lambda x: x['date'])

        total_return = (self.final_state.portfolio_value / self.config.initial_capital - 1) if self.final_state else 0
        win_trades = sum(1 for t in all_trades if t.get('pnl', 0) > 0)

        max_drawdown = 0.0
        if all_daily_returns:
            cumulative_values = [self.config.initial_capital]
            for dr in all_daily_returns:
                cumulative_values.append(cumulative_values[-1] * (1 + dr['daily_return']))

            peak = cumulative_values[0]
            for value in cumulative_values:
                if value > peak:
                    peak = value
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

        sharpe_ratio = 0.0
        if all_daily_returns:
            returns = [dr['daily_return'] for dr in all_daily_returns]
            if len(returns) > 1:
                std_return = np.std(returns)
                if std_return > 0:
                    sharpe_ratio = (np.mean(returns) / std_return) * np.sqrt(252)

        return {
            'start_date': self.batch_results[0].start_date,
            'end_date': self.batch_results[-1].end_date,
            'initial_capital': self.config.initial_capital,
            'final_value': self.final_state.portfolio_value if self.final_state else self.config.initial_capital,
            'total_return': total_return,
            'total_return_pct': total_return * 100,
            'total_trades': len(all_trades),
            'win_trades': win_trades,
            'loss_trades': len(all_trades) - win_trades,
            'win_rate': win_trades / len(all_trades) * 100 if all_trades else 0,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown * 100,
            'sharpe_ratio': sharpe_ratio,
            'trades': all_trades,
            'daily_returns': all_daily_returns,
            'portfolio_values': [dr['portfolio_value'] for dr in all_daily_returns] if all_daily_returns else [],
        }

    def _save_final_result(self, result: Dict):
        """保存最终结果"""
        json_result = {k: v for k, v in result.items() if not isinstance(v, list)}

        json_file = self.output_dir / 'final_result.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_result, f, indent=2, default=str)

        pkl_file = self.output_dir / 'final_result.pkl'
        with open(pkl_file, 'wb') as f:
            pickle.dump(result, f)

        if result.get('trades'):
            trades_df = pd.DataFrame(result['trades'])
            trades_file = self.output_dir / 'trades.csv'
            trades_df.to_csv(trades_file, index=False, encoding='utf-8-sig')

        logger.info(f"\n最终结果已保存到: {self.output_dir}")


try:
    from backtest.vectorized_backtest_engine import (
        VectorizedBacktestEngine,
        VectorizedBacktestConfig,
    )
    HAS_VECTORIZED_ENGINE = True
except ImportError:
    HAS_VECTORIZED_ENGINE = False


def check_vectorized_support() -> bool:
    """检查向量化回测是否可用"""
    return HAS_VECTORIZED_ENGINE


def get_recommended_engine(data_size: int = None, memory_limit_gb: float = 8.0) -> str:
    """推荐合适的回测引擎"""
    if not HAS_VECTORIZED_ENGINE:
        return '增量批处理'
    if data_size is None:
        return '向量化'
    n_symbols = data_size // 250
    estimated_memory_gb = n_symbols * 250 * 3 * 8 / (1024**3)
    if estimated_memory_gb < memory_limit_gb * 0.7:
        return '向量化'
    return '增量批处理'
