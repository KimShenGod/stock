#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
流式回测策略 - 基于预计算信号查表（纯 Python 实现）

功能:
1. 使用预计算的信号矩阵，逐日查表获取买卖信号
2. 无需在回测时重新计算技术指标
3. 支持增量加载信号数据
4. 无 AKQuant 依赖，纯 Python 实现

使用方式:
    1. 先使用 SignalCalculator 预计算信号并保存
    2. 回测时加载信号矩阵
    3. 每日查表获取当日触发的信号
"""

import os
import pickle
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Union
from dataclasses import dataclass

import pandas as pd
import numpy as np

from backtest.local_data_loader import LocalDataLoader

logger = logging.getLogger(__name__)


@dataclass
class StreamingStrategyConfig:
    """流式策略配置"""
    initial_capital: float = 100000.0
    max_positions: int = 10

    buy_signal_type: str = '周线MACD区间_buy'
    sell_signal_type: str = ''

    buy_time: str = 'open'
    sell_time: str = 'open'

    max_hold_days: int = 20
    stop_loss_pct: float = -0.07
    stop_profit_pct: float = 0.10

    commission_rate: float = 0.0003
    min_commission: float = 5.0
    stamp_tax_rate: float = 0.001

    min_signal_strength: float = 1.0
    top_n_select: int = 5

    signal_buffer_days: int = 5


class SignalLookupManager:
    """
    信号查表管理器

    管理预计算的信号数据，支持增量加载和查表
    """

    def __init__(
        self,
        signal_dir: str,
        start_date: str,
        end_date: str,
        buffer_days: int = 5,
    ):
        self.signal_dir = Path(signal_dir)
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.buffer_days = buffer_days

        self._signal_cache: Optional[pd.DataFrame] = None
        self._cache_start: Optional[datetime] = None
        self._cache_end: Optional[datetime] = None

        self.signal_files = sorted(self.signal_dir.glob('signals_batch_*.pkl'))
        if not self.signal_files:
            raise FileNotFoundError(f"未找到信号文件: {signal_dir}")

        self._load_config()
        logger.info(f"信号查表管理器初始化完成，共 {len(self.signal_files)} 个批次")

    def _load_config(self):
        """加载信号配置"""
        config_path = self.signal_dir / 'signal_config.pkl'
        if config_path.exists():
            with open(config_path, 'rb') as f:
                config_data = pickle.load(f)
            self.signal_config = config_data.get('config')
            self.available_signals = config_data.get('signal_types', [])
            logger.info(f"信号配置加载成功，可用信号: {self.available_signals}")
        else:
            self.signal_config = None
            self.available_signals = []
            logger.warning("未找到信号配置文件")

    def _load_signals_for_range(self, start: datetime, end: datetime) -> pd.DataFrame:
        """加载指定日期范围的信号"""
        all_signals = []

        for filepath in self.signal_files:
            df = pd.read_pickle(filepath)
            if not df.empty:
                df = df[(df.index >= start) & (df.index <= end)]
            if not df.empty:
                all_signals.append(df)

        if not all_signals:
            return pd.DataFrame()

        combined = pd.concat(all_signals, ignore_index=False)
        combined = combined.sort_index()
        return combined

    def ensure_date_loaded(self, date: datetime):
        """确保指定日期的信号已加载"""
        if self._signal_cache is not None:
            if self._cache_start <= date <= self._cache_end:
                return

        new_start = max(date - timedelta(days=self.buffer_days), self.start_date)
        new_end = min(date + timedelta(days=self.buffer_days * 2), self.end_date)

        logger.info(f"加载信号数据: {new_start.date()} ~ {new_end.date()}")

        if self._signal_cache is not None:
            del self._signal_cache
            import gc
            gc.collect()

        self._signal_cache = self._load_signals_for_range(new_start, new_end)
        self._cache_start = new_start
        self._cache_end = new_end

        logger.info(f"信号缓存加载完成: {len(self._signal_cache)} 行")

    def get_signals_for_date(
        self,
        date: Union[str, datetime],
        signal_type: str,
        min_value: float = 1.0,
    ) -> List[str]:
        """
        获取某一天触发指定信号的股票列表

        Args:
            date: 日期
            signal_type: 信号类型（如 '周线MACD区间_buy'）
            min_value: 最小信号值

        Returns:
            股票代码列表
        """
        date = pd.to_datetime(date)
        self.ensure_date_loaded(date)

        if self._signal_cache is None or self._signal_cache.empty:
            return []

        day_signals = self._signal_cache[self._signal_cache.index.date == date.date()]

        if day_signals.empty:
            return []

        if signal_type not in day_signals.columns:
            logger.warning(f"信号类型 {signal_type} 不在数据中")
            return []

        triggered = day_signals[day_signals[signal_type] >= min_value]
        symbols = triggered['symbol'].unique().tolist()

        return symbols

    def get_all_signals_for_date(self, date: Union[str, datetime]) -> pd.DataFrame:
        """获取某一天的所有信号"""
        date = pd.to_datetime(date)
        self.ensure_date_loaded(date)

        if self._signal_cache is None:
            return pd.DataFrame()

        day_signals = self._signal_cache[self._signal_cache.index.date == date.date()]
        return day_signals.copy()

    def get_signal_strength(
        self,
        date: Union[str, datetime],
        symbol: str,
        signal_type: str,
    ) -> float:
        """获取某股票某日的信号强度"""
        date = pd.to_datetime(date)
        self.ensure_date_loaded(date)

        if self._signal_cache is None:
            return 0.0

        mask = (
            (self._signal_cache.index.date == date.date()) &
            (self._signal_cache['symbol'] == symbol)
        )

        row = self._signal_cache[mask]

        if row.empty or signal_type not in row.columns:
            return 0.0

        return float(row[signal_type].iloc[0])

    def clear_cache(self):
        """清空缓存释放内存"""
        if self._signal_cache is not None:
            del self._signal_cache
            self._signal_cache = None
            self._cache_start = None
            self._cache_end = None
            import gc
            gc.collect()
            logger.info("信号缓存已清空")


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    signal_dir = './backtest_results/signals'

    if os.path.exists(signal_dir):
        manager = SignalLookupManager(signal_dir, '20240101', '20241231')
        buy_signals = manager.get_signals_for_date('2024-06-01', '周线MACD区间_buy')
        print(f"2024-06-01 买入信号股票: {buy_signals}")
    else:
        print(f"信号目录不存在: {signal_dir}")