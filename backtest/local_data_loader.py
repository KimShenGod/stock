#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
本地数据加载器 - 从 pickle 目录直接读取本地数据

适配目标项目数据格式:
- 数据路径: user_config.tdx['pickle']
- 列名: date, code, name, open, high, low, close, vol, amount, 换手率, 流通市值
- 股票代码格式: 000001 (纯数字)

功能:
1. 支持 pickle 格式的本地数据读取
2. 增量加载 - 按日期范围分批加载
3. 内存缓存管理 - LRU 缓存释放
4. 返回标准格式的 DataFrame

使用示例:
    loader = LocalDataLoader()
    df = loader.load_single('000001', start_date='20240101', end_date='20241231')
    df_dict = loader.load_batch(['000001', '000002'], start_date='20240101', end_date='20241231')
"""

import os
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Union, Tuple
from collections import OrderedDict

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


class LocalDataLoader:
    """
    本地数据加载器 - 从 pickle 目录加载 K 线数据
    """

    COLUMN_MAP = {
        'date': 'date',
        'code': 'symbol',
        'name': 'name',
        'open': 'open',
        'high': 'high',
        'low': 'low',
        'close': 'close',
        'vol': 'volume',
        'volume': 'volume',
        'amount': 'amount',
        '换手率': 'turnover_rate',
        '流通市值': 'circulation_mv',
    }

    def __init__(
        self,
        data_dir: str = None,
        cache_size: int = 100,
    ):
        if data_dir is None:
            try:
                import user_config as ucfg
                data_dir = ucfg.tdx['pickle']
            except ImportError:
                current_dir = Path(__file__).parent
                possible_paths = [
                    current_dir.parent.parent / 'sourcecode' / 'stock-analysis-master' / 'TDXdata' / 'pickle',
                    Path('./TDXdata/pickle'),
                    Path('../TDXdata/pickle'),
                ]
                for path in possible_paths:
                    if path.exists():
                        data_dir = str(path)
                        break
                if data_dir is None:
                    raise FileNotFoundError("未找到数据目录，请指定 data_dir")

        self.data_dir = Path(data_dir)
        self.cache_size = cache_size
        self._cache: OrderedDict[str, pd.DataFrame] = OrderedDict()

        if not self.data_dir.exists():
            raise FileNotFoundError(f"数据目录不存在: {self.data_dir}")

        self.available_symbols = self._scan_available_symbols()
        logger.info(f"数据加载器初始化完成，共 {len(self.available_symbols)} 只股票可用")

    def _scan_available_symbols(self) -> List[str]:
        symbols = set()
        for file in self.data_dir.glob('*.pkl'):
            symbol = file.stem
            if symbol.startswith(('sh', 'sz', 'bj')):
                symbol = symbol[2:]
            if symbol.isdigit() and len(symbol) == 6:
                symbols.add(symbol)
        return sorted(list(symbols))

    def _normalize_symbol(self, symbol: str) -> str:
        symbol = symbol.strip().lower()
        if symbol.startswith(('sh', 'sz', 'bj')):
            return symbol[2:]
        return symbol

    def _load_raw_data(self, symbol: str) -> Optional[pd.DataFrame]:
        symbol = self._normalize_symbol(symbol)

        if symbol in self._cache:
            self._cache.move_to_end(symbol)
            return self._cache[symbol].copy()

        pickle_path = self.data_dir / f"{symbol}.pkl"
        if not pickle_path.exists():
            return None

        try:
            df = pd.read_pickle(pickle_path)
        except Exception as e:
            logger.warning(f"加载 pickle 失败 {symbol}: {e}")
            return None

        if df is None or df.empty:
            return None

        df = self._standardize_columns(df, symbol)

        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.set_index('date')

        df = df.sort_index()
        self._add_to_cache(symbol, df)
        return df.copy()

    def _standardize_columns(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        df = df.copy()
        rename_map = {}
        for col in df.columns:
            col_lower = col.lower() if isinstance(col, str) else col
            if col in self.COLUMN_MAP:
                rename_map[col] = self.COLUMN_MAP[col]
            elif col_lower in self.COLUMN_MAP:
                rename_map[col] = self.COLUMN_MAP[col_lower]
        if rename_map:
            df = df.rename(columns=rename_map)
        if 'symbol' not in df.columns:
            df['symbol'] = symbol
        return df

    def _add_to_cache(self, symbol: str, df: pd.DataFrame):
        if len(self._cache) >= self.cache_size:
            self._cache.popitem(last=False)
        self._cache[symbol] = df

    def clear_cache(self):
        self._cache.clear()

    def load_single(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[List[str]] = None,
    ) -> Optional[pd.DataFrame]:
        df = self._load_raw_data(symbol)
        if df is None or df.empty:
            return None
        if start_date:
            df = df[df.index >= pd.to_datetime(start_date)]
        if end_date:
            df = df[df.index <= pd.to_datetime(end_date)]
        if columns:
            required = ['symbol']
            for col in required:
                if col not in columns:
                    columns = columns + [col]
            available = [c for c in columns if c in df.columns]
            df = df[available]
        return df.copy() if not df.empty else None

    def load_batch(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        columns: Optional[List[str]] = None,
        as_dict: bool = True,
    ) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
        results = {}
        for symbol in symbols:
            df = self.load_single(symbol, start_date, end_date, columns)
            if df is not None and not df.empty:
                results[symbol] = df
        if as_dict:
            return results
        if not results:
            return pd.DataFrame()
        combined = pd.concat(results.values(), ignore_index=False)
        combined = combined.sort_index()
        return combined

    def get_date_range(self, symbol: str) -> Optional[Tuple[str, str]]:
        df = self._load_raw_data(symbol)
        if df is None or df.empty:
            return None
        return (
            df.index[0].strftime('%Y%m%d'),
            df.index[-1].strftime('%Y%m%d')
        )

    def get_available_dates(self) -> pd.DatetimeIndex:
        if not self.available_symbols:
            return pd.DatetimeIndex([])
        df = self._load_raw_data(self.available_symbols[0])
        if df is None:
            return pd.DatetimeIndex([])
        return df.index.copy()

    def split_date_range(
        self,
        start_date: str,
        end_date: str,
        batch_days: int = 90,
    ) -> List[Tuple[str, str]]:
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)
        date_ranges = []
        current = start
        while current <= end:
            batch_end = min(current + pd.Timedelta(days=batch_days), end)
            date_ranges.append((
                current.strftime('%Y%m%d'),
                batch_end.strftime('%Y%m%d')
            ))
            current = batch_end + pd.Timedelta(days=1)
        return date_ranges


class StreamingDataFeed:
    """流式数据源 - 逐日迭代提供数据"""

    def __init__(self, data_loader: LocalDataLoader, symbols: List[str],
                 start_date: str, end_date: str):
        self.loader = data_loader
        self.symbols = symbols
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        self.trading_days = self._get_trading_days()
        self.current_idx = 0

    def _get_trading_days(self) -> pd.DatetimeIndex:
        all_dates = self.loader.get_available_dates()
        mask = (all_dates >= self.start_date) & (all_dates <= self.end_date)
        return all_dates[mask]

    def __iter__(self):
        self.current_idx = 0
        return self

    def __next__(self) -> Tuple[datetime, pd.DataFrame]:
        if self.current_idx >= len(self.trading_days):
            raise StopIteration
        date = self.trading_days[self.current_idx]
        date_str = date.strftime('%Y%m%d')
        df = self.loader.load_batch(
            self.symbols, start_date=date_str, end_date=date_str, as_dict=False
        )
        self.current_idx += 1
        return date, df

    def __len__(self) -> int:
        return len(self.trading_days)


def load_stock_data(symbol: str, start_date: str = None,
                    end_date: str = None, data_dir: str = None) -> Optional[pd.DataFrame]:
    loader = LocalDataLoader(data_dir=data_dir)
    return loader.load_single(symbol, start_date, end_date)


def get_stock_list(data_dir: str = None) -> List[str]:
    loader = LocalDataLoader(data_dir=data_dir)
    return loader.available_symbols


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    loader = LocalDataLoader()
    print(f"\n可用股票数量: {len(loader.available_symbols)}")
    print(f"前 10 只: {loader.available_symbols[:10]}")
    if loader.available_symbols:
        symbol = loader.available_symbols[0]
        df = loader.load_single(symbol, '20240101', '20241231')
        if df is not None:
            print(f"数据范围: {df.index[0]} ~ {df.index[-1]}")
            print(f"列名: {df.columns.tolist()}")
            print(df.head())
