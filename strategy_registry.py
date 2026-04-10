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

# ============== 策略实现 ==============

@register_strategy("高开涨停")
def high_open_limit_up_strategy(df, start_date='', end_date='', mode=None):
    """
    高开涨停策略：T-1日涨停 + T日高开2%以上
    """
    try:
        close_price = df['close']
        open_price = df['open']
        high_price = df.get('high', close_price)
        
        prev_close = close_price.shift(1)
        prev2_close = close_price.shift(2)
        prev_high = high_price.shift(1)
        
        # T-1日涨停（前前日收盘价 * 1.1）
        prev_limit_up_price = prev2_close * 1.1
        prev_limit_up = prev_high >= prev_limit_up_price * 0.995
        
        # T日高开2%以上
        today_gap_up = open_price >= prev_close * 1.02
        
        signal = prev_limit_up & today_gap_up
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("前日涨停")
def prev_day_limit_up_strategy(df, start_date='', end_date='', mode=None):
    """
    前日涨停策略：T-1日涨停
    """
    try:
        close_price = df['close']
        high_price = df.get('high', close_price)
        
        prev_close = close_price.shift(1)
        prev_high = high_price.shift(1)
        
        # T-1日涨停
        prev_limit_up_price = prev_close * 1.1
        signal = prev_high >= prev_limit_up_price * 0.995
        
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("今日涨停")
def today_limit_up_strategy(df, start_date='', end_date='', mode=None):
    """
    今日涨停策略：今日涨停
    """
    try:
        close_price = df['close']
        prev_close = close_price.shift(1)
        
        # 今日涨停
        signal = close_price >= prev_close * 1.095
        
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("小市值")
def small_market_cap_strategy(df, start_date='', end_date='', mode=None):
    """
    小市值策略：流通市值 < 100亿
    """
    try:
        if '流通市值' in df.columns:
            signal = df['流通市值'] < 100e8
        elif 'market_cap' in df.columns:
            signal = df['market_cap'] < 100e8
        else:
            signal = pd.Series(True, index=df.index)  # 无数据时默认满足
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("换手率")
def turnover_rate_strategy(df, start_date='', end_date='', mode=None):
    """
    换手率策略：换手率 > 3%
    """
    try:
        if '换手率' in df.columns:
            signal = df['换手率'] > 3
        else:
            signal = pd.Series(False, index=df.index)
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("MACD周线金叉")
def macd_weekly_golden_cross_strategy(df, start_date='', end_date='', mode=None):
    """
    MACD周线金叉策略
    """
    try:
        close_price = df['close']
        
        # 简化的MACD计算
        ema12 = close_price.ewm(span=12, adjust=False).mean()
        ema26 = close_price.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        macd = (dif - dea) * 2
        
        # 金叉：dif 从下方穿越 dea
        signal = (dif > dea) & (dif.shift(1) <= dea.shift(1))
        
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("MACD日线金叉")
def macd_daily_golden_cross_strategy(df, start_date='', end_date='', mode=None):
    """
    MACD日线金叉策略
    """
    try:
        close_price = df['close']
        
        # 简化的MACD计算
        ema12 = close_price.ewm(span=12, adjust=False).mean()
        ema26 = close_price.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        dea = dif.ewm(span=9, adjust=False).mean()
        
        # 金叉
        signal = (dif > dea) & (dif.shift(1) <= dea.shift(1))
        
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("连续上涨")
def continuous_rise_strategy(df, start_date='', end_date='', mode=None):
    """
    连续上涨策略：连续3天上涨
    """
    try:
        close_price = df['close']
        
        # 每天的涨跌
        daily_change = close_price > close_price.shift(1)
        
        # 连续3天上涨
        signal = daily_change & daily_change.shift(1) & daily_change.shift(2)
        
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


@register_strategy("MACD周线区间")
def macd_weekly_range_strategy(df, start_date='', end_date='', mode=None):
    """
    MACD周线区间策略：DIF在一定区间内
    """
    try:
        close_price = df['close']
        
        # 简化的MACD
        ema12 = close_price.ewm(span=12, adjust=False).mean()
        ema26 = close_price.ewm(span=26, adjust=False).mean()
        dif = ema12 - ema26
        
        # DIF 在 0 到 1 之间
        signal = (dif > 0) & (dif < 1)
        
        return signal.fillna(False)
    except Exception:
        return pd.Series(False, index=df.index)


# 添加别名，支持 config.yml 中的名称
_STRATEGY_REGISTRY["周线MACD区间"] = macd_weekly_range_strategy
