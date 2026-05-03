"""
策略注册系统
用于回测时自动注册和组合选股策略

使用方法：
1. 在 huice_bt_vbt.py 中导入本模块
2. 在 config.yml 中配置策略组合
"""

import numpy as np
import pandas as pd
from func_TDX import REF, MA, HHV, LLV

# ============== 策略注册表 ==============
_STRATEGY_REGISTRY = {}

def register_strategy(name):
    """
    策略注册装饰器
    用法：
    @register_strategy("高开涨停")
    def my_strategy(df, ...):
        ...
    """
    def decorator(func):
        _STRATEGY_REGISTRY[name] = func
        return func
    return decorator

def get_strategy(name):
    """根据名称获取策略函数"""
    return _STRATEGY_REGISTRY.get(name)

def list_strategies():
    """列出所有已注册的策略"""
    return list(_STRATEGY_REGISTRY.keys())

# ============== 通用工具 ==============

def _get_limit_up_pct(stock_code):
    """根据股票代码判断涨停幅度"""
    if stock_code.startswith('68') or stock_code.startswith('30'):
        return 0.2
    return 0.1


def _sort_ascending(df):
    """将DataFrame按日期升序排列"""
    df_copy = df.copy()
    if isinstance(df_copy.index, pd.DatetimeIndex):
        df_copy = df_copy.sort_index()
    else:
        df_copy = df_copy.sort_values('date')
    return df_copy


def _to_original_index(daily_signals, df_asc, df):
    """将升序信号映射回原始df索引"""
    return pd.Series(daily_signals, index=df_asc.index).reindex(df.index).fillna(False).astype(bool)


# ============== 回测逐日信号计算 ==============

def _daily_limit_up(df_asc):
    """逐日涨停信号：每天的最高价是否达到涨停价"""
    n = len(df_asc)
    if n < 2:
        return np.zeros(n, dtype=bool)
    close = df_asc['close'].values
    high = df_asc['high'].values
    stock_code = df_asc['code'].iloc[0] if 'code' in df_asc.columns else ''
    pct = _get_limit_up_pct(stock_code)
    signals = np.zeros(n, dtype=bool)
    for i in range(1, n):
        if high[i] >= close[i - 1] * (1 + pct) * 0.995:
            signals[i] = True
    return signals


def _daily_high_open_limit_up(df_asc):
    """逐日高开涨停信号：前一天涨停 + 当天高开2%+"""
    n = len(df_asc)
    if n < 3:
        return np.zeros(n, dtype=bool)
    close = df_asc['close'].values
    high = df_asc['high'].values
    open_price = df_asc['open'].values
    stock_code = df_asc['code'].iloc[0] if 'code' in df_asc.columns else ''
    pct = _get_limit_up_pct(stock_code)
    signals = np.zeros(n, dtype=bool)
    for i in range(2, n):
        prev_limit = high[i - 1] >= close[i - 2] * (1 + pct) * 0.995
        gap_up = open_price[i] >= close[i - 1] * 1.02
        if prev_limit and gap_up:
            signals[i] = True
    return signals


def _daily_macd_golden_cross(df_asc, window=5):
    """逐日MACD金叉信号：最近window天内是否出现金叉且在零轴上方"""
    n = len(df_asc)
    if n < 26:
        return np.zeros(n, dtype=bool)
    dif, dea = _calculate_macd(df_asc['close'])
    if dif.isnull().all() or dea.isnull().all():
        return np.zeros(n, dtype=bool)
    prev_dif = dif.shift(1)
    prev_dea = dea.shift(1)
    golden_cross = (dif > dea) & (prev_dif <= prev_dea) & (dif > 0) & (dea > 0)
    return golden_cross.rolling(window, min_periods=1).max().fillna(0).astype(bool).values


def _daily_continuous_rise(df_asc, n_days=3):
    """逐日连续上涨信号：连续n天上涨"""
    if len(df_asc) < n_days:
        return np.zeros(len(df_asc), dtype=bool)
    close = df_asc['close']
    daily_up = close > close.shift(1)
    return daily_up.rolling(n_days, min_periods=n_days).min().fillna(False).astype(bool).values


def _daily_macd_range(df_asc):
    """逐日MACD区间信号"""
    n = len(df_asc)
    if n < 26:
        return np.zeros(n, dtype=bool)
    dif, dea = _calculate_macd(df_asc['close'])
    if dif.isnull().all() or dea.isnull().all():
        return np.zeros(n, dtype=bool)
    hist = dif - dea
    in_range = (dif > 0.05) & (dif < 0.3) & (dea > 0.05) & (dea < 0.3)
    increasing = (hist > hist.shift(1)) & (hist.shift(1) > hist.shift(2))
    return (in_range & increasing).fillna(False).astype(bool).values


# ============== MACD计算 ==============

def _calculate_macd(close_price):
    """计算MACD指标，优先使用talib，回退到pandas ewm"""
    try:
        import talib
        macd, macdsignal, macdhist = talib.MACD(
            close_price.values, fastperiod=12, slowperiod=26, signalperiod=9
        )
        return pd.Series(macd, index=close_price.index), pd.Series(macdsignal, index=close_price.index)
    except ImportError:
        pass
    ema12 = close_price.ewm(span=12, adjust=False).mean()
    ema26 = close_price.ewm(span=26, adjust=False).mean()
    dif = ema12 - ema26
    dea = dif.ewm(span=9, adjust=False).mean()
    return dif, dea


# ============== 单日检查（dashboard用） ==============

def _check_limit_up(df_asc, row_idx):
    """检查升序数据中row_idx行是否涨停"""
    if row_idx < 1:
        return False
    stock_code = df_asc['code'].iloc[0] if 'code' in df_asc.columns else ''
    pct = _get_limit_up_pct(stock_code)
    return df_asc.iloc[row_idx]['high'] >= df_asc.iloc[row_idx - 1]['close'] * (1 + pct) * 0.995


def _check_macd_gold_cross_latest(df_asc, window=5):
    """检查最近window天是否有金叉（dashboard用）"""
    if len(df_asc) < 26:
        return False
    dif, dea = _calculate_macd(df_asc['close'])
    if dif.isnull().all() or dea.isnull().all():
        return False
    prev_dif = dif.shift(1)
    prev_dea = dea.shift(1)
    golden_cross = (dif > dea) & (prev_dif <= prev_dea) & (dif > 0) & (dea > 0)
    return golden_cross.tail(window).any()


# ============== 策略实现 ==============

def _make_strategy(daily_fn, latest_fn, desc):
    """策略工厂：mode='backtest'用逐日信号，否则只检查最新日"""
    def wrapper(df, start_date='', end_date='', mode=None):
        try:
            if mode == 'backtest':
                df_asc = _sort_ascending(df)
                return _to_original_index(daily_fn(df_asc), df_asc, df)
            else:
                return latest_fn(df)
        except Exception:
            return pd.Series(False, index=df.index)
    wrapper.__doc__ = desc
    return wrapper


def _latest_high_open_limit_up(df):
    """最新一天的高开涨停检查"""
    if len(df) < 3:
        return pd.Series(False, index=df.index)
    df_asc = _sort_ascending(df)
    n = len(df_asc)
    stock_code = df_asc['code'].iloc[0] if 'code' in df_asc.columns else ''
    pct = _get_limit_up_pct(stock_code)
    prev_limit = df_asc.iloc[-2]['high'] >= df_asc.iloc[-3]['close'] * (1 + pct) * 0.995
    gap_up = df_asc.iloc[-1]['open'] >= df_asc.iloc[-2]['close'] * 1.02
    result = pd.Series(False, index=df.index)
    if prev_limit and gap_up:
        result.iloc[0] = True
    return result


def _latest_prev_limit_up(df):
    """最新一天的前日涨停检查"""
    if len(df) < 3:
        return pd.Series(False, index=df.index)
    df_asc = _sort_ascending(df)
    result = pd.Series(False, index=df.index)
    if _check_limit_up(df_asc, len(df_asc) - 2):
        result.iloc[0] = True
    return result


def _latest_today_limit_up(df):
    """最新一天的今日涨停检查"""
    if len(df) < 2:
        return pd.Series(False, index=df.index)
    df_asc = _sort_ascending(df)
    result = pd.Series(False, index=df.index)
    if _check_limit_up(df_asc, len(df_asc) - 1):
        result.iloc[0] = True
    return result


def _latest_small_market_cap(df):
    """最新一天的小市值检查"""
    try:
        if '流通市值' in df.columns:
            signal = df['流通市值'] < 100e8
        elif 'market_cap' in df.columns:
            signal = df['market_cap'] < 100e8
        else:
            signal = pd.Series(True, index=df.index)
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


def _latest_turnover_rate(df):
    """最新一天的换手率检查"""
    try:
        if '换手率' in df.columns:
            signal = df['换手率'] > 3
        else:
            signal = pd.Series(False, index=df.index)
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


def _latest_macd_weekly_golden_cross(df):
    """最新的MACD周线金叉检查"""
    df_copy = _sort_ascending(df)
    if len(df_copy) < 30:
        return pd.Series(False, index=df.index)
    result = pd.Series(False, index=df.index)
    if _check_macd_gold_cross_latest(df_copy, window=5):
        result.iloc[0] = True
    return result


def _latest_macd_daily_golden_cross(df):
    """最新的MACD日线金叉检查"""
    df_copy = _sort_ascending(df)
    if len(df_copy) < 26:
        return pd.Series(False, index=df.index)
    result = pd.Series(False, index=df.index)
    if _check_macd_gold_cross_latest(df_copy, window=5):
        result.iloc[0] = True
    return result


def _latest_continuous_rise(df):
    """最新的连续上涨检查"""
    try:
        df_asc = _sort_ascending(df)
        if len(df_asc) < 3:
            return pd.Series(False, index=df.index)
        close = df_asc['close']
        daily_up = close > close.shift(1)
        signal = daily_up & daily_up.shift(1) & daily_up.shift(2)
        result = pd.Series(False, index=df.index)
        if signal.iloc[-1]:
            result.iloc[0] = True
        return result
    except Exception:
        return pd.Series(False, index=df.index)


def _latest_macd_weekly_range(df):
    """最新的MACD周线区间检查"""
    try:
        df_asc = _sort_ascending(df)
        if len(df_asc) < 28:
            return pd.Series(False, index=df.index)
        dif, dea = _calculate_macd(df_asc['close'])
        if dif.isnull().all() or dea.isnull().all():
            return pd.Series(False, index=df.index)
        hist = dif - dea
        recent_dif = dif.tail(3)
        recent_dea = dea.tail(3)
        in_range = (recent_dif.iloc[-1] > 0.05) & (recent_dif.iloc[-1] < 0.3) & \
                    (recent_dea.iloc[-1] > 0.05) & (recent_dea.iloc[-1] < 0.3)
        increasing = recent_dif.iloc[-1] > recent_dif.iloc[-2] > recent_dif.iloc[-3]
        result = pd.Series(False, index=df.index)
        if in_range and increasing:
            result.iloc[0] = True
        return result
    except Exception:
        return pd.Series(False, index=df.index)


# ============== 注册所有策略 ==============

register_strategy("高开涨停")(_make_strategy(
    _daily_high_open_limit_up, _latest_high_open_limit_up,
    "高开涨停策略：T-1日涨停 + T日高开2%以上"
))
register_strategy("前日涨停")(_make_strategy(
    _daily_limit_up, _latest_prev_limit_up,
    "前日涨停策略：T-1日涨停"
))
register_strategy("今日涨停")(_make_strategy(
    _daily_limit_up, _latest_today_limit_up,
    "今日涨停策略：T日涨停"
))
register_strategy("小市值")(_make_strategy(
    lambda df_asc: (df_asc['流通市值'] < 100e8).fillna(False).values if '流通市值' in df_asc.columns
    else (df_asc['market_cap'] < 100e8).fillna(False).values if 'market_cap' in df_asc.columns
    else np.ones(len(df_asc), dtype=bool),
    _latest_small_market_cap,
    "小市值策略：流通市值 < 100亿"
))
register_strategy("换手率")(_make_strategy(
    lambda df_asc: (df_asc['换手率'] > 3).fillna(False).values if '换手率' in df_asc.columns
    else np.zeros(len(df_asc), dtype=bool),
    _latest_turnover_rate,
    "换手率策略：换手率 > 3%"
))
register_strategy("MACD周线金叉")(_make_strategy(
    lambda df_asc: _daily_macd_golden_cross(df_asc, window=5),
    _latest_macd_weekly_golden_cross,
    "MACD周线金叉策略：最近5周有金叉且零轴上方"
))
register_strategy("MACD日线金叉")(_make_strategy(
    lambda df_asc: _daily_macd_golden_cross(df_asc, window=5),
    _latest_macd_daily_golden_cross,
    "MACD日线金叉策略：最近5天有金叉且零轴上方"
))
register_strategy("连续上涨")(_make_strategy(
    lambda df_asc: _daily_continuous_rise(df_asc, n_days=3),
    _latest_continuous_rise,
    "连续上涨策略：连续3天上涨"
))
register_strategy("MACD周线区间")(_make_strategy(
    _daily_macd_range,
    _latest_macd_weekly_range,
    "MACD周线区间策略：DIF和DEA在0.05~0.3且MACD柱放大"
))

_STRATEGY_REGISTRY["周线MACD区间"] = _STRATEGY_REGISTRY["MACD周线区间"]
