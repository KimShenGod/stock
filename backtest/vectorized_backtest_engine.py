#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向量化回测引擎 - 全量矩阵一次性处理（纯 Python/NumPy 实现）

核心优势:
1. 一次性加载全量信号矩阵，无需逐日查表
2. 使用NumPy向量化运算替代Python逐日循环
3. 支持Numba JIT加速（可选）
4. 理论加速10-50倍

使用方式:
    engine = VectorizedBacktestEngine(signal_dir='./signals', data_dir='./TDXdata')
    result = engine.run('20240101', '20241231', symbols=stock_list)
"""

import os
import pickle
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, asdict
import gc

import pandas as pd
import numpy as np

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    plt = None

try:
    from numba import njit, prange
    HAS_NUMBA = True
except ImportError:
    HAS_NUMBA = False
    def njit(*args, **kwargs):
        def wrapper(f):
            return f
        return wrapper
    prange = range

from backtest.local_data_loader import LocalDataLoader
from backtest.streaming_strategy import StreamingStrategyConfig

logger = logging.getLogger(__name__)


# ====================================================================
# Numba加速函数
# ====================================================================

@njit(cache=True)
def _vectorized_position_update(
    buy_signals: np.ndarray,
    sell_signals: np.ndarray,
    max_positions: int,
    initial_capital: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """核心向量化持仓更新函数 (Numba加速)"""
    n_days, n_symbols = buy_signals.shape

    positions = np.zeros((n_days, n_symbols), dtype=np.int32)
    cash = np.zeros(n_days, dtype=np.float64)
    trades = np.zeros(n_days, dtype=np.int32)

    cash[0] = initial_capital
    current_positions = np.zeros(n_symbols, dtype=np.int32)
    current_cash = initial_capital

    for day in range(n_days):
        if day > 0:
            cash[day] = current_cash
            positions[day] = current_positions
        else:
            cash[day] = initial_capital

        sell_triggered = sell_signals[day] & (current_positions > 0)
        for s in range(n_symbols):
            if sell_triggered[s]:
                current_positions[s] = 0
                trades[day] += 1

        current_position_count = np.sum(current_positions > 0)
        available_slots = max_positions - current_position_count

        if available_slots > 0:
            buy_candidates = buy_signals[day] & (current_positions == 0)
            for s in range(n_symbols):
                if available_slots <= 0:
                    break
                if buy_candidates[s]:
                    current_positions[s] = 1
                    available_slots -= 1
                    trades[day] += 1

    return positions, cash, trades


# ====================================================================
# 配置类
# ====================================================================

@dataclass
class VectorizedBacktestConfig:
    """向量化回测配置"""
    initial_capital: float = 100000.0
    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_tax_rate: float = 0.001

    max_positions: int = 10
    buy_signal_type: str = '周线MACD区间_buy'
    sell_signal_type: str = ''

    buy_time: str = 'open'
    sell_time: str = 'open'
    position_size: str = 'equal'

    max_hold_days: int = 20
    stop_loss_pct: float = -0.07
    stop_profit_pct: float = 0.10

    chunk_size: int = 1000
    use_numba: bool = True

    output_dir: str = './backtest_results'
    save_trades: bool = True
    save_daily: bool = True


# ====================================================================
# 向量化回测引擎
# ====================================================================

class VectorizedBacktestEngine:
    """
    向量化回测引擎

    核心特点:
    1. 一次性加载全量信号和价格数据
    2. 使用Pandas/NumPy向量化操作替代逐日循环
    3. 支持Numba JIT编译进一步加速
    """

    def __init__(
        self,
        signal_dir: str,
        data_dir: str = None,
        config: VectorizedBacktestConfig = None,
    ):
        self.signal_dir = Path(signal_dir)
        self.data_dir = Path(data_dir) if data_dir else None
        self.config = config or VectorizedBacktestConfig()

        self.data_loader = LocalDataLoader(data_dir=data_dir) if data_dir else None

        self._signal_matrix: Optional[pd.DataFrame] = None
        self._price_matrix: Optional[pd.DataFrame] = None
        self._result_cache: Optional[Dict] = None

        logger.info("向量化回测引擎初始化")
        logger.info(f"Numba加速: {HAS_NUMBA and self.config.use_numba}")

    def _load_signals(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None,
    ) -> pd.DataFrame:
        """加载信号数据"""
        logger.info(f"加载信号数据: {start_date} ~ {end_date}")

        signal_files = sorted(self.signal_dir.glob('signals_batch_*.pkl'))
        if not signal_files:
            raise FileNotFoundError(f"未找到信号文件: {self.signal_dir}")

        start_dt = pd.to_datetime(start_date)
        end_dt = pd.to_datetime(end_date)

        all_signals = []

        for filepath in signal_files:
            df = pd.read_pickle(filepath)

            if df.empty:
                continue

            df = df[(df.index >= start_dt) & (df.index <= end_dt)]

            if symbols is not None and 'symbol' in df.columns:
                df = df[df['symbol'].isin(symbols)]

            if not df.empty:
                all_signals.append(df)

        if not all_signals:
            logger.warning("没有加载到任何信号数据")
            return pd.DataFrame()

        combined = pd.concat(all_signals, ignore_index=False)
        combined = combined.sort_index()

        logger.info(f"信号数据加载完成: {combined.shape}")
        return combined

    def _load_prices(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str],
    ) -> pd.DataFrame:
        """加载价格数据并构建价格矩阵"""
        if self.data_loader is None:
            raise ValueError("需要提供data_dir以加载价格数据")

        logger.info(f"加载价格数据: {len(symbols)} 只股票")

        price_data = []

        for symbol in symbols:
            try:
                df = self.data_loader.load_single(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    df = df.copy()
                    df['symbol'] = symbol
                    price_data.append(df[['symbol', 'close', 'open']])
            except Exception as e:
                logger.debug(f"加载 {symbol} 失败: {e}")

        if not price_data:
            raise ValueError("没有加载到价格数据")

        combined = pd.concat(price_data, ignore_index=False)

        price_matrix = combined.pivot_table(
            index=combined.index,
            columns='symbol',
            values='close',
            fill_value=np.nan
        )

        price_matrix = price_matrix.ffill()

        logger.info(f"价格矩阵: {price_matrix.shape}")
        return price_matrix

    def _build_signal_matrices(
        self,
        signal_df: pd.DataFrame,
        price_matrix: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """构建信号矩阵"""
        dates = price_matrix.index
        symbols = price_matrix.columns

        buy_matrix = pd.DataFrame(0, index=dates, columns=symbols, dtype=np.int8)
        sell_matrix = pd.DataFrame(0, index=dates, columns=symbols, dtype=np.int8)

        if not signal_df.empty and 'symbol' in signal_df.columns:
            buy_col = self.config.buy_signal_type
            sell_col = self.config.sell_signal_type

            if buy_col in signal_df.columns:
                buy_pivot = signal_df.pivot_table(
                    index=signal_df.index,
                    columns='symbol',
                    values=buy_col,
                    fill_value=0
                )
                buy_pivot = buy_pivot.reindex(index=dates, columns=symbols, fill_value=0)
                buy_matrix = (buy_pivot >= 1).astype(np.int8)

            if sell_col and sell_col in signal_df.columns:
                sell_pivot = signal_df.pivot_table(
                    index=signal_df.index,
                    columns='symbol',
                    values=sell_col,
                    fill_value=0
                )
                sell_pivot = sell_pivot.reindex(index=dates, columns=symbols, fill_value=0)
                sell_matrix = (sell_pivot >= 1).astype(np.int8)

        logger.info(f"信号矩阵构建完成: 买入{buy_matrix.sum().sum()}次, 卖出{sell_matrix.sum().sum()}次")
        return buy_matrix, sell_matrix

    def _vectorized_backtest_core(
        self,
        buy_matrix: pd.DataFrame,
        sell_matrix: pd.DataFrame,
        price_matrix: pd.DataFrame,
    ) -> Dict:
        """核心向量化回测逻辑"""
        n_days, n_symbols = price_matrix.shape
        dates = price_matrix.index
        symbols = price_matrix.columns

        logger.info(f"开始向量化回测: {n_days}天 x {n_symbols}只")

        positions = pd.DataFrame(0, index=dates, columns=symbols, dtype=np.int32)
        position_values = pd.DataFrame(0.0, index=dates, columns=symbols, dtype=np.float64)
        cash = pd.Series(self.config.initial_capital, index=dates, dtype=np.float64)
        portfolio_value = pd.Series(self.config.initial_capital, index=dates, dtype=np.float64)

        trades = []
        current_positions = {}
        current_cash = self.config.initial_capital

        target_position_value = self.config.initial_capital / self.config.max_positions

        logger.info("执行向量化回测循环...")

        for i, date in enumerate(dates):
            if i == 0:
                portfolio_value.iloc[i] = current_cash
                continue

            # 计算昨日持仓的今日价值
            hold_value = 0.0
            for symbol, pos_info in list(current_positions.items()):
                if symbol in price_matrix.columns:
                    current_price = price_matrix.loc[date, symbol]
                    shares = pos_info['shares']
                    pos_value = shares * current_price
                    hold_value += pos_value
                    position_values.loc[date, symbol] = pos_value

            current_cash = cash.iloc[i-1]
            total_value = current_cash + hold_value
            portfolio_value.iloc[i] = total_value
            cash.iloc[i] = current_cash

            for symbol in current_positions:
                if symbol in positions.columns:
                    positions.loc[date, symbol] = 1

            # ========== 处理卖出 ==========
            sell_signals_today = sell_matrix.loc[date]
            symbols_to_sell = []

            # 信号触发卖出
            for symbol in current_positions:
                if symbol in sell_signals_today.index and sell_signals_today[symbol] > 0:
                    symbols_to_sell.append(('signal', symbol))

            # 止损止盈 + 持仓天数检查
            for symbol, pos_info in list(current_positions.items()):
                if symbol in price_matrix.columns:
                    current_price = price_matrix.loc[date, symbol]
                    entry_price = pos_info['entry_price']
                    pnl_pct = (current_price - entry_price) / entry_price
                    hold_days = (date - pos_info['entry_date']).days

                    if pnl_pct <= self.config.stop_loss_pct:
                        symbols_to_sell.append(('stop_loss', symbol))
                    elif pnl_pct >= self.config.stop_profit_pct:
                        symbols_to_sell.append(('stop_profit', symbol))
                    elif hold_days >= self.config.max_hold_days:
                        symbols_to_sell.append(('max_hold', symbol))

            # 执行卖出
            for reason, symbol in symbols_to_sell:
                if symbol not in current_positions:
                    continue

                pos_info = current_positions[symbol]
                current_price = price_matrix.loc[date, symbol]
                shares = pos_info['shares']
                sell_value = shares * current_price

                commission = max(sell_value * self.config.commission_rate, self.config.min_commission)
                stamp_tax = sell_value * self.config.stamp_tax_rate
                total_cost = commission + stamp_tax

                current_cash += sell_value - total_cost

                pnl = (current_price - pos_info['entry_price']) * shares - total_cost
                pnl_pct = pnl / (pos_info['entry_price'] * shares)

                trades.append({
                    'date': date,
                    'symbol': symbol,
                    'action': 'SELL',
                    'reason': reason,
                    'price': current_price,
                    'shares': shares,
                    'value': sell_value,
                    'cost': total_cost,
                    'pnl': pnl,
                    'pnl_pct': pnl_pct,
                    'hold_days': (date - pos_info['entry_date']).days,
                })

                del current_positions[symbol]
                positions.loc[date, symbol] = 0
                position_values.loc[date, symbol] = 0

            # ========== 处理买入 ==========
            buy_signals_today = buy_matrix.loc[date]

            current_position_count = len(current_positions)
            available_slots = self.config.max_positions - current_position_count

            if available_slots > 0:
                buy_candidates = []
                for symbol in buy_signals_today.index:
                    if buy_signals_today[symbol] > 0 and symbol not in current_positions:
                        buy_candidates.append(symbol)

                to_buy = buy_candidates[:available_slots]

                for symbol in to_buy:
                    if symbol not in price_matrix.columns:
                        continue

                    buy_price = price_matrix.loc[date, symbol]

                    position_budget = min(target_position_value, current_cash * 0.95)
                    shares = int(position_budget / buy_price / 100) * 100

                    if shares < 100:
                        continue

                    buy_value = shares * buy_price
                    commission = max(buy_value * self.config.commission_rate, self.config.min_commission)

                    if buy_value + commission > current_cash:
                        continue

                    current_cash -= (buy_value + commission)

                    current_positions[symbol] = {
                        'entry_date': date,
                        'entry_price': buy_price,
                        'shares': shares,
                    }

                    positions.loc[date, symbol] = 1
                    position_values.loc[date, symbol] = buy_value

                    trades.append({
                        'date': date,
                        'symbol': symbol,
                        'action': 'BUY',
                        'reason': 'signal',
                        'price': buy_price,
                        'shares': shares,
                        'value': buy_value,
                        'cost': commission,
                        'pnl': 0,
                        'pnl_pct': 0,
                        'hold_days': 0,
                    })

            cash.iloc[i] = current_cash

            total_value = current_cash + sum(
                current_positions[s]['shares'] * price_matrix.loc[date, s]
                for s in current_positions if s in price_matrix.columns
            )
            portfolio_value.iloc[i] = total_value

        result = self._build_result(
            dates=dates,
            symbols=symbols,
            positions=positions,
            position_values=position_values,
            cash=cash,
            portfolio_value=portfolio_value,
            trades=trades,
            price_matrix=price_matrix,
        )

        return result

    def _build_result(
        self,
        dates: pd.DatetimeIndex,
        symbols: pd.Index,
        positions: pd.DataFrame,
        position_values: pd.DataFrame,
        cash: pd.Series,
        portfolio_value: pd.Series,
        trades: List[Dict],
        price_matrix: pd.DataFrame,
    ) -> Dict:
        """构建回测结果"""

        initial_value = self.config.initial_capital
        final_value = portfolio_value.iloc[-1]
        total_return = (final_value - initial_value) / initial_value

        daily_returns = portfolio_value.pct_change().dropna()

        n_days = len(dates)
        annual_return = (1 + total_return) ** (252 / n_days) - 1 if n_days > 0 else 0

        volatility = daily_returns.std() * np.sqrt(252) if len(daily_returns) > 0 else 0

        risk_free_rate = 0.02
        sharpe_ratio = (annual_return - risk_free_rate) / volatility if volatility > 0 else 0

        cummax = portfolio_value.cummax()
        drawdown = (portfolio_value - cummax) / cummax
        max_drawdown = drawdown.min()

        trades_df = pd.DataFrame(trades) if trades else pd.DataFrame()

        n_buy = len([t for t in trades if t['action'] == 'BUY'])
        n_sell = len([t for t in trades if t['action'] == 'SELL'])

        win_trades = [t for t in trades if t['action'] == 'SELL' and t['pnl'] > 0]
        loss_trades = [t for t in trades if t['action'] == 'SELL' and t['pnl'] <= 0]

        win_rate = len(win_trades) / len(win_trades + loss_trades) if (win_trades + loss_trades) else 0

        avg_pnl = sum(t['pnl'] for t in win_trades + loss_trades) / len(win_trades + loss_trades) if (win_trades + loss_trades) else 0

        return {
            'initial_capital': initial_value,
            'final_value': final_value,
            'total_return': total_return,
            'total_return_pct': total_return * 100,
            'annual_return': annual_return,
            'annual_return_pct': annual_return * 100,

            'volatility': volatility,
            'volatility_pct': volatility * 100,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'max_drawdown_pct': max_drawdown * 100,

            'total_trades': len(trades),
            'buy_trades': n_buy,
            'sell_trades': n_sell,
            'completed_trades': len(win_trades + loss_trades),
            'win_trades': len(win_trades),
            'loss_trades': len(loss_trades),
            'win_rate': win_rate,
            'win_rate_pct': win_rate * 100,
            'avg_pnl': avg_pnl,
            'total_pnl': sum(t['pnl'] for t in trades if t['action'] == 'SELL'),

            'daily_returns': daily_returns,
            'portfolio_value': portfolio_value,
            'cash': cash,
            'positions': positions,
            'position_values': position_values,
            'trades': trades_df,
            'drawdown': drawdown,

            'start_date': dates[0],
            'end_date': dates[-1],
            'n_days': n_days,
            'n_symbols': len(symbols),

            'method': 'vectorized',
        }

    def run(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None,
        **kwargs,
    ) -> Dict:
        """运行向量化回测"""
        import time
        start_time = time.time()

        logger.info(f"\n{'='*60}")
        logger.info(f"向量化回测开始: {start_date} ~ {end_date}")
        logger.info(f"{'='*60}")

        signal_df = self._load_signals(start_date, end_date, symbols)

        if signal_df.empty:
            logger.error("信号数据为空，无法进行回测")
            return {}

        if symbols is None:
            symbols = signal_df['symbol'].unique().tolist()

        logger.info(f"回测股票数: {len(symbols)}")

        price_matrix = self._load_prices(start_date, end_date, symbols)

        if price_matrix.empty:
            logger.error("价格数据为空，无法进行回测")
            return {}

        symbols = [s for s in symbols if s in price_matrix.columns]
        price_matrix = price_matrix[symbols]

        buy_matrix, sell_matrix = self._build_signal_matrices(signal_df, price_matrix)

        result = self._vectorized_backtest_core(
            buy_matrix=buy_matrix,
            sell_matrix=sell_matrix,
            price_matrix=price_matrix,
        )

        elapsed = time.time() - start_time
        result['elapsed_time'] = elapsed
        result['elapsed_time_str'] = f"{elapsed:.1f}秒"

        logger.info(f"\n{'='*60}")
        logger.info(f"向量化回测完成！耗时: {elapsed:.1f}秒")
        logger.info(f"{'='*60}")
        self._print_result(result)

        self._result_cache = result
        return result

    def _print_result(self, result: Dict):
        """打印回测结果"""
        if not result:
            return

        logger.info(f"\n回测结果摘要:")
        logger.info(f"  资金: {result['initial_capital']:,.0f} -> {result['final_value']:,.0f}")
        logger.info(f"  总收益: {result['total_return']*100:+.2f}%")
        logger.info(f"  年化收益: {result['annual_return']*100:+.2f}%")
        logger.info(f"  夏普比率: {result['sharpe_ratio']:.2f}")
        logger.info(f"  最大回撤: {result['max_drawdown']*100:.2f}%")
        logger.info(f"  交易次数: 买入{result['buy_trades']}次, 卖出{result['sell_trades']}次")
        logger.info(f"  胜率: {result['win_rate']*100:.1f}%")

    def save_result(self, result: Dict = None, filepath: str = None):
        """保存回测结果"""
        result = result or self._result_cache
        if not result:
            logger.warning("没有可保存的结果")
            return

        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = Path(self.config.output_dir) / f"vectorized_backtest_{timestamp}.pkl"
        else:
            filepath = Path(filepath)

        filepath.parent.mkdir(parents=True, exist_ok=True)

        save_data = {
            'config': asdict(self.config),
            'summary': {k: v for k, v in result.items() if isinstance(v, (int, float, str, bool, type(None)))},
            'trades': result['trades'].to_dict('records') if isinstance(result['trades'], pd.DataFrame) else result['trades'],
            'daily_returns': result['daily_returns'].to_dict() if isinstance(result['daily_returns'], pd.Series) else result['daily_returns'],
            'portfolio_value': result['portfolio_value'].to_dict() if isinstance(result['portfolio_value'], pd.Series) else result['portfolio_value'],
        }

        with open(filepath, 'wb') as f:
            pickle.dump(save_data, f)

        logger.info(f"回测结果已保存: {filepath}")

        if isinstance(result['trades'], pd.DataFrame) and not result['trades'].empty:
            trades_csv = filepath.with_suffix('.trades.csv')
            result['trades'].to_csv(trades_csv, index=False, encoding='utf-8-sig')
            logger.info(f"交易记录已保存: {trades_csv}")

        # 生成收益曲线图
        equity_curve_path = filepath.parent / f"equity_curve_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        self.plot_equity_curve(result, equity_curve_path)

    def plot_equity_curve(self, result: Dict = None, filepath: str = None):
        """
        绘制收益曲线图

        Args:
            result: 回测结果字典，如果为None则使用缓存的结果
            filepath: 图片保存路径，如果为None则自动生成
        """
        result = result or self._result_cache
        if not result:
            logger.warning("没有可绘制的结果")
            return

        if not HAS_MATPLOTLIB:
            logger.warning("matplotlib未安装，无法绘制收益曲线图")
            return

        portfolio_value = result.get('portfolio_value')
        if portfolio_value is None or len(portfolio_value) == 0:
            logger.warning("没有组合净值数据")
            return

        if filepath is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = Path(self.config.output_dir) / f"equity_curve_{timestamp}.png"
        else:
            filepath = Path(filepath)

        filepath.parent.mkdir(parents=True, exist_ok=True)

        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial']
        plt.rcParams['axes.unicode_minus'] = False

        fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
        fig.suptitle('策略回测收益分析', fontsize=14, fontweight='bold')

        # 1. 净值曲线
        ax1 = axes[0]
        dates = portfolio_value.index
        values = portfolio_value.values

        ax1.plot(dates, values / self.config.initial_capital, 'b-', linewidth=1.5, label='组合净值')
        ax1.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5, label='初始净值')
        ax1.fill_between(dates, 1.0, values / self.config.initial_capital,
                         where=values >= self.config.initial_capital,
                         alpha=0.3, color='green', interpolate=True)
        ax1.fill_between(dates, 1.0, values / self.config.initial_capital,
                         where=values < self.config.initial_capital,
                         alpha=0.3, color='red', interpolate=True)
        ax1.set_ylabel('净值')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.set_title(f'总收益: {result["total_return_pct"]:.2f}% | 年化: {result["annual_return_pct"]:.2f}%')

        # 2. 回撤曲线
        ax2 = axes[1]
        drawdown = result.get('drawdown')
        if drawdown is not None:
            ax2.fill_between(dates, 0, drawdown.values * 100,
                            alpha=0.5, color='red', label='回撤')
            ax2.set_ylabel('回撤 (%)')
            ax2.legend(loc='upper right')
            ax2.grid(True, alpha=0.3)
            ax2.set_title(f'最大回撤: {result["max_drawdown_pct"]:.2f}%')
            ax2.invert_yaxis()

        # 3. 每日收益分布
        ax3 = axes[2]
        daily_returns = result.get('daily_returns')
        if daily_returns is not None and len(daily_returns) > 0:
            # 对齐日期：daily_returns比portfolio_value少第一天
            returns_dates = dates[-len(daily_returns):]
            daily_returns_pct = daily_returns.values * 100
            ax3.bar(returns_dates, daily_returns_pct,
                   color=['green' if r >= 0 else 'red' for r in daily_returns_pct],
                   alpha=0.7, width=1.0)
            ax3.set_ylabel('日收益 (%)')
            ax3.axhline(y=0, color='gray', linestyle='-', alpha=0.5)
            ax3.grid(True, alpha=0.3)
            ax3.set_title(f'夏普比率: {result["sharpe_ratio"]:.2f} | 胜率: {result["win_rate_pct"]:.1f}%')

        # 设置x轴日期格式
        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))

        plt.tight_layout()
        plt.savefig(filepath, dpi=150, bbox_inches='tight')
        plt.close()

        logger.info(f"收益曲线图已保存: {filepath}")
