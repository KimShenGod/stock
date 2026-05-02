#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向量化回测引擎

基于离线预计算架构的回测系统，支持 17.5x ~ 70x+ 加速。

核心组件:
- LocalDataLoader: 本地数据加载器，适配项目 pickle 数据格式
- SignalCalculator: 离线信号预计算，桥接现有策略函数
- VectorizedBacktestEngine: 向量化回测引擎（推荐）
- BatchBacktestEngine: 增量批处理引擎（断点续传）

快速开始:
    from backtest import quick_backtest
    result = quick_backtest(start_date='20240101', end_date='20241231')
    print(f"收益: {result['total_return_pct']:.2f}%")
"""

from backtest.local_data_loader import (
    LocalDataLoader,
    StreamingDataFeed,
    load_stock_data,
    get_stock_list,
)

from backtest.signal_calculator import (
    SignalCalculator,
    SignalConfig,
    SIGNAL_CALCULATORS,
)

from backtest.streaming_strategy import (
    SignalLookupManager,
    StreamingStrategyConfig,
)

from backtest.vectorized_backtest_engine import (
    VectorizedBacktestEngine,
    VectorizedBacktestConfig,
)

from backtest.batch_backtest_engine import (
    IncrementalBacktestEngine,
    BatchBacktestConfig,
    check_vectorized_support,
    get_recommended_engine,
)


def quick_backtest(
    start_date: str = '20240101',
    end_date: str = '20241231',
    strategy_combo: str = 'default',
    max_positions: int = 3,
    initial_capital: float = 400000.0,
    stop_loss: float = 0.08,
    take_profit: float = 0.30,
    max_hold_days: int = 30,
    use_vectorized: bool = True,
    signal_dir: str = None,
    output_dir: str = './backtest_results',
) -> dict:
    """
    快速回测接口

    自动检查并计算信号，使用向量化引擎进行回测。

    Args:
        start_date: 开始日期 (YYYYMMDD)
        end_date: 结束日期 (YYYYMMDD)
        strategy_combo: 策略组合名称 (在 config.yml 中定义)
        max_positions: 最大持仓数
        initial_capital: 初始资金
        stop_loss: 止损比例 (如 0.08 = 8%)
        take_profit: 止盈比例 (如 0.30 = 30%)
        max_hold_days: 最大持仓天数
        use_vectorized: 是否使用向量化引擎
        signal_dir: 信号存储目录
        output_dir: 结果输出目录

    Returns:
        回测结果字典
    """
    import os
    from pathlib import Path

    if signal_dir is None:
        signal_dir = str(Path(output_dir) / 'signals')

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    strategy_config = StreamingStrategyConfig(
        initial_capital=initial_capital,
        max_positions=max_positions,
        stop_loss_pct=-stop_loss,
        stop_profit_pct=take_profit,
        max_hold_days=max_hold_days,
    )

    if use_vectorized and check_vectorized_support():
        # 获取数据路径
        try:
            import user_config as ucfg
            data_dir = ucfg.tdx.get('pickle', None)
        except ImportError:
            data_dir = None

        engine = VectorizedBacktestEngine(
            signal_dir=signal_dir,
            data_dir=data_dir,
            config=VectorizedBacktestConfig(
                initial_capital=initial_capital,
                max_positions=max_positions,
                stop_loss_pct=-stop_loss,
                stop_profit_pct=take_profit,
                max_hold_days=max_hold_days,
                output_dir=output_dir,
            ),
        )

        result = engine.run(
            start_date=start_date,
            end_date=end_date,
            strategy_config=strategy_config,
        )
    else:
        batch_config = BatchBacktestConfig(
            signal_dir=signal_dir,
            output_dir=output_dir,
            initial_capital=initial_capital,
        )

        engine = IncrementalBacktestEngine(
            config=batch_config,
            strategy_config=strategy_config,
        )

        result = engine.run(
            start_date=start_date,
            end_date=end_date,
        )

    return result


__all__ = [
    'LocalDataLoader',
    'StreamingDataFeed',
    'load_stock_data',
    'get_stock_list',
    'SignalCalculator',
    'SignalConfig',
    'SIGNAL_CALCULATORS',
    'SignalLookupManager',
    'StreamingStrategyConfig',
    'VectorizedBacktestEngine',
    'VectorizedBacktestConfig',
    'IncrementalBacktestEngine',
    'BatchBacktestConfig',
    'check_vectorized_support',
    'get_recommended_engine',
    'quick_backtest',
]
