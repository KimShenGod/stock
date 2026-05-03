#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
离线信号预计算模块 - 桥接策略注册表

功能:
1. 将 strategy_registry 中注册的策略函数转换为信号矩阵
2. 支持分批计算和保存
3. 回测时只需查表，无需重复计算

适配目标项目:
- 桥接 strategy_registry.get_strategy() 返回的函数
- 信号格式: {strategy_name}_buy (int 0|1)
- 数据格式: date(DatetimeIndex), symbol(str)

使用示例:
    calculator = SignalCalculator()
    calculator.calculate_and_save_batch(symbols, config, strategy_names=['周线MACD区间', '小市值'])
"""

import os
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union, Tuple
from dataclasses import dataclass

import pandas as pd
import numpy as np

from backtest.local_data_loader import LocalDataLoader

logger = logging.getLogger(__name__)


@dataclass
class SignalConfig:
    """信号计算配置"""
    start_date: str
    end_date: str
    lookback_days: int = 120
    batch_size: int = 100


def calculate_registered_strategy_signals(
    df: pd.DataFrame,
    config: SignalConfig,
    strategy_name: str,
) -> Optional[pd.DataFrame]:
    """
    将已注册的策略函数包装为信号计算

    Args:
        df: 股票日线数据 DataFrame
        config: 信号配置
        strategy_name: 策略名称（需在 strategy_registry 中注册）

    Returns:
        信号 DataFrame，包含 {strategy_name}_buy 列
    """
    try:
        # 尝试从 strategy_registry 获取策略函数
        try:
            from strategy_registry import get_strategy
            strategy_func = get_strategy(strategy_name)
        except ImportError:
            logger.warning("无法导入 strategy_registry")
            return None

        if strategy_func is None:
            logger.warning(f"策略 '{strategy_name}' 未注册")
            return None

        # 调用策略函数
        # 策略签名: func(df, start_date='', end_date='', mode=None) -> bool | pd.Series
        try:
            result = strategy_func(df, config.start_date, config.end_date, mode='backtest')
        except Exception as e:
            logger.debug(f"策略 {strategy_name} 执行失败: {e}")
            return None

        # 转换结果为信号格式
        signals = pd.DataFrame(index=df.index)
        signal_col = f'{strategy_name}_buy'

        if isinstance(result, bool):
            # 单一布尔值 -> 最后一天设置信号
            signals[signal_col] = 0
            if result and len(signals) > 0:
                signals[signal_col].iloc[-1] = 1
        elif isinstance(result, pd.Series):
            # 布尔序列 -> 直接转换为整数
            signals[signal_col] = result.astype(int).fillna(0)
        else:
            logger.warning(f"策略 {strategy_name} 返回类型不支持: {type(result)}")
            return None

        return signals

    except Exception as e:
        logger.error(f"计算策略 {strategy_name} 信号失败: {e}")
        return None


def get_registered_strategies() -> List[str]:
    """获取所有已注册的策略名称"""
    try:
        from strategy_registry import list_strategies
        return list_strategies()
    except ImportError:
        # 如果无法导入，返回 config.yml 中定义的默认策略
        return [
            '高开涨停', '前日涨停', '今日涨停', '小市值',
            '换手率', 'MACD周线金叉', 'MACD日线金叉',
            '连续上涨', 'MACD周线区间', '周线MACD区间'
        ]


# 信号计算器映射 - 动态从 strategy_registry 注册
SIGNAL_CALCULATORS: Dict[str, callable] = {}


def _init_signal_calculators():
    """初始化信号计算器映射"""
    global SIGNAL_CALCULATORS
    for name in get_registered_strategies():
        SIGNAL_CALCULATORS[name] = lambda df, cfg, n=name: calculate_registered_strategy_signals(df, cfg, n)


# 模块加载时初始化
_init_signal_calculators()


class SignalCalculator:
    """
    批量信号计算器

    功能:
    1. 批量计算多只股票的信号
    2. 将信号保存为矩阵格式
    3. 支持增量计算和分批保存
    """

    def __init__(
        self,
        data_dir: str = None,
        output_dir: str = None,
    ):
        self.data_loader = LocalDataLoader(data_dir=data_dir)

        if output_dir is None:
            output_dir = str(self.data_loader.data_dir / 'signals')
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"信号计算器初始化完成，输出目录: {self.output_dir}")

    def calculate_signals(
        self,
        symbols: List[str],
        config: SignalConfig,
        strategy_names: List[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        """
        批量计算信号

        Args:
            symbols: 股票代码列表
            config: 信号配置
            strategy_names: 策略名称列表，默认使用所有注册策略

        Returns:
            Dict[symbol, DataFrame] - 每个股票的信号
        """
        if strategy_names is None:
            strategy_names = list(SIGNAL_CALCULATORS.keys())

        logger.info(f"开始计算 {len(symbols)} 只股票的 {len(strategy_names)} 种信号...")

        results = {}

        for i, symbol in enumerate(symbols):
            if (i + 1) % 100 == 0:
                logger.info(f"进度: {i + 1}/{len(symbols)}")

            signals = self._calculate_single_stock_signals(symbol, config, strategy_names)
            if signals is not None and not signals.empty:
                results[symbol] = signals

        logger.info(f"信号计算完成，成功 {len(results)}/{len(symbols)}")
        return results

    def _calculate_single_stock_signals(
        self,
        symbol: str,
        config: SignalConfig,
        strategy_names: List[str],
    ) -> Optional[pd.DataFrame]:
        """计算单个股票的所有信号"""
        try:
            # 加载历史数据
            start_dt = pd.to_datetime(config.start_date) - timedelta(days=config.lookback_days)
            df = self.data_loader.load_single(
                symbol,
                start_date=start_dt.strftime('%Y%m%d'),
                end_date=config.end_date,
            )

            if df is None or len(df) < 30:
                return None

            # 合并所有信号
            all_signals = pd.DataFrame(index=df.index)
            all_signals['symbol'] = symbol

            for strategy_name in strategy_names:
                if strategy_name in SIGNAL_CALCULATORS:
                    signals = SIGNAL_CALCULATORS[strategy_name](df, config)
                    if signals is not None and not signals.empty:
                        all_signals = pd.concat([all_signals, signals], axis=1)

            # 只保留指定日期范围
            start_dt = pd.to_datetime(config.start_date)
            end_dt = pd.to_datetime(config.end_date)
            all_signals = all_signals[(all_signals.index >= start_dt) & (all_signals.index <= end_dt)]

            return all_signals if not all_signals.empty else None

        except Exception as e:
            logger.error(f"计算 {symbol} 信号失败: {e}")
            return None

    def calculate_and_save_batch(
        self,
        symbols: List[str],
        config: SignalConfig,
        strategy_names: List[str] = None,
        batch_size: int = 100,
    ):
        """
        分批计算并保存信号

        Args:
            symbols: 股票代码列表
            config: 信号配置
            strategy_names: 策略名称列表
            batch_size: 每批次的股票数量
        """
        if strategy_names is None:
            strategy_names = list(SIGNAL_CALCULATORS.keys())

        # 清空旧的信号文件
        for old_file in self.output_dir.glob('signals_batch_*.pkl'):
            old_file.unlink()
            logger.info(f"删除旧信号文件: {old_file}")

        total = len(symbols)
        n_batches = (total + batch_size - 1) // batch_size

        logger.info(f"分批计算信号: 共 {total} 只，每批 {batch_size} 只，共 {n_batches} 批")

        for i in range(n_batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, total)
            batch_symbols = symbols[start_idx:end_idx]

            logger.info(f"处理第 {i+1}/{n_batches} 批: {start_idx+1}-{end_idx}")

            batch_signals = self.calculate_signals(batch_symbols, config, strategy_names)
            self._save_signal_batch(batch_signals, i, n_batches, config, strategy_names)

            del batch_signals
            import gc
            gc.collect()

        logger.info(f"分批计算完成，信号保存在: {self.output_dir}")

    def _save_signal_batch(
        self,
        signals: Dict[str, pd.DataFrame],
        batch_idx: int,
        total_batches: int,
        config: SignalConfig,
        strategy_names: List[str],
    ):
        """保存单个批次的信号"""
        if not signals:
            return

        combined = pd.concat(signals.values(), ignore_index=False)
        combined = combined.sort_index()

        filename = f"signals_batch_{batch_idx:04d}_of_{total_batches:04d}.pkl"
        filepath = self.output_dir / filename

        combined.to_pickle(filepath)

        config_path = self.output_dir / 'signal_config.pkl'
        with open(config_path, 'wb') as f:
            pickle.dump({
                'config': config,
                'strategy_names': strategy_names,
                'signal_types': list(signals.values())[0].columns.tolist() if signals else [],
                'created_at': datetime.now().isoformat(),
            }, f)

        logger.info(f"批次 {batch_idx+1} 保存完成: {filepath} ({len(signals)} 只股票)")

    def load_signal_matrix(
        self,
        start_date: str = None,
        end_date: str = None,
        symbols: List[str] = None,
    ) -> pd.DataFrame:
        """
        加载信号矩阵

        Returns:
            信号矩阵 DataFrame
        """
        signal_files = sorted(self.output_dir.glob('signals_batch_*.pkl'))

        if not signal_files:
            raise FileNotFoundError(f"未找到信号文件: {self.output_dir}")

        logger.info(f"加载 {len(signal_files)} 个批次的信号文件...")

        all_signals = []
        for filepath in signal_files:
            df = pd.read_pickle(filepath)

            if start_date:
                df = df[df.index >= pd.to_datetime(start_date)]
            if end_date:
                df = df[df.index <= pd.to_datetime(end_date)]
            if symbols:
                df = df[df['symbol'].isin(symbols)]

            if not df.empty:
                all_signals.append(df)

        if not all_signals:
            return pd.DataFrame()

        combined = pd.concat(all_signals, ignore_index=False)
        combined = combined.sort_index()

        logger.info(f"信号矩阵加载完成: {len(combined)} 行 x {len(combined.columns)} 列")
        return combined

    def get_signal_for_date(
        self,
        date: Union[str, datetime],
        signal_type: str,
    ) -> List[str]:
        """
        获取某一天的信号股票列表

        Args:
            date: 日期
            signal_type: 信号类型（策略名称）

        Returns:
            触发该信号的股票列表
        """
        date_str = pd.to_datetime(date).strftime('%Y-%m-%d')
        signals = self.load_signal_matrix(start_date=date_str, end_date=date_str)

        if signals.empty:
            return []

        # 兼容策略名称格式
        signal_col = f'{signal_type}_buy'
        if signal_col not in signals.columns:
            return []

        triggered = signals[signals[signal_col] == 1]['symbol'].tolist()
        return triggered


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    calculator = SignalCalculator()
    symbols = calculator.data_loader.available_symbols[:10]
    print(f"测试股票: {symbols}")

    config = SignalConfig(start_date='20240101', end_date='20241231')
    calculator.calculate_and_save_batch(symbols, config, batch_size=5)