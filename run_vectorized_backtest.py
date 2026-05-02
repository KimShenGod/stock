#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
向量化回测命令行入口

用法:
    # 预计算信号
    python run_vectorized_backtest.py --mode precompute --start 20240101 --end 20241231

    # 运行回测
    python run_vectorized_backtest.py --mode backtest --strategy-combo default

    # 全流程（预计算 + 回测）
    python run_vectorized_backtest.py --mode full --start 20240101 --end 20241231
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime

import yaml

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_config():
    """加载配置文件"""
    config_path = Path(__file__).parent / 'config.yml'
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}


def get_strategy_combo(config: dict, combo_name: str) -> list:
    """获取策略组合"""
    strategies = config.get('strategies', {})
    return strategies.get(combo_name, strategies.get('default', ['周线MACD区间', '小市值']))


def precompute_signals(
    start_date: str,
    end_date: str,
    strategy_names: list,
    signal_dir: str,
    max_stocks: int = None,
):
    """预计算信号"""
    from backtest import SignalCalculator, SignalConfig

    logger.info(f"\n{'='*60}")
    logger.info(f"预计算信号: {start_date} ~ {end_date}")
    logger.info(f"策略: {strategy_names}")
    logger.info(f"{'='*60}")

    calculator = SignalCalculator(output_dir=signal_dir)
    symbols = calculator.data_loader.available_symbols

    if max_stocks:
        symbols = symbols[:max_stocks]
        logger.info(f"限制股票数量: {max_stocks}")

    logger.info(f"共 {len(symbols)} 只股票")

    config = SignalConfig(
        start_date=start_date,
        end_date=end_date,
        batch_size=100,
    )

    calculator.calculate_and_save_batch(
        symbols,
        config,
        strategy_names=strategy_names,
        batch_size=50,
    )

    logger.info(f"\n信号预计算完成，保存在: {signal_dir}")


def run_backtest(
    start_date: str,
    end_date: str,
    strategy_combo: str,
    config: dict,
    use_vectorized: bool = True,
):
    """运行回测"""
    from backtest import (
        VectorizedBacktestEngine,
        VectorizedBacktestConfig,
        IncrementalBacktestEngine,
        BatchBacktestConfig,
        StreamingStrategyConfig,
        check_vectorized_support,
    )

    # 获取数据路径
    try:
        import user_config as ucfg
        data_dir = ucfg.tdx.get('pickle', './TDXdata/pickle')
    except ImportError:
        data_dir = './TDXdata/pickle'

    backtest_config = config.get('backtest', {})
    initial_capital = backtest_config.get('initial_capital', 400000)
    max_positions = backtest_config.get('max_positions', 3)
    stop_loss = backtest_config.get('stop_loss', 0.08)
    take_profit = backtest_config.get('take_profit', 0.30)
    max_hold_days = backtest_config.get('max_hold_days', 30)
    signal_dir = backtest_config.get('signal_dir', './backtest_results/signals')
    output_dir = backtest_config.get('output_dir', './backtest_results')

    strategy_names = get_strategy_combo(config, strategy_combo)
    buy_signal_type = f'{strategy_names[0]}_buy' if strategy_names else '周线MACD区间_buy'

    logger.info(f"\n{'='*60}")
    logger.info(f"运行回测: {start_date} ~ {end_date}")
    logger.info(f"策略组合: {strategy_combo} -> {strategy_names}")
    logger.info(f"买入信号: {buy_signal_type}")
    logger.info(f"{'='*60}")

    strategy_config = StreamingStrategyConfig(
        initial_capital=initial_capital,
        max_positions=max_positions,
        stop_loss_pct=-stop_loss,
        stop_profit_pct=take_profit,
        max_hold_days=max_hold_days,
        buy_signal_type=buy_signal_type,
    )

    if use_vectorized and check_vectorized_support():
        logger.info("使用向量化回测引擎")

        engine = VectorizedBacktestEngine(
            signal_dir=signal_dir,
            data_dir=data_dir,
            config=VectorizedBacktestConfig(
                initial_capital=initial_capital,
                max_positions=max_positions,
                stop_loss_pct=-stop_loss,
                stop_profit_pct=take_profit,
                max_hold_days=max_hold_days,
                buy_signal_type=buy_signal_type,
                output_dir=output_dir,
            ),
        )

        result = engine.run(start_date, end_date)
        engine.save_result()

    else:
        logger.info("使用增量批处理回测引擎")

        batch_config = BatchBacktestConfig(
            signal_dir=signal_dir,
            output_dir=output_dir,
            initial_capital=initial_capital,
        )

        engine = IncrementalBacktestEngine(
            config=batch_config,
            strategy_config=strategy_config,
        )

        result = engine.run(start_date, end_date)

    return result


def main():
    parser = argparse.ArgumentParser(description='向量化回测系统')

    parser.add_argument('--mode', type=str, default='full',
                        choices=['precompute', 'backtest', 'full'],
                        help='运行模式: precompute(预计算信号), backtest(回测), full(全流程)')

    parser.add_argument('--start', type=str, default='20240101',
                        help='开始日期 (YYYYMMDD)')

    parser.add_argument('--end', type=str, default='20241231',
                        help='结束日期 (YYYYMMDD)')

    parser.add_argument('--strategy-combo', type=str, default='default',
                        help='策略组合名称 (在 config.yml 中定义)')

    parser.add_argument('--max-stocks', type=int, default=None,
                        help='最大股票数量 (用于测试)')

    parser.add_argument('--no-vectorized', action='store_true',
                        help='禁用向量化引擎，使用增量批处理')

    args = parser.parse_args()

    config = load_config()
    backtest_config = config.get('backtest', {})
    signal_dir = backtest_config.get('signal_dir', './backtest_results/signals')

    Path(signal_dir).mkdir(parents=True, exist_ok=True)

    strategy_names = get_strategy_combo(config, args.strategy_combo)

    if args.mode == 'precompute':
        precompute_signals(
            start_date=args.start,
            end_date=args.end,
            strategy_names=strategy_names,
            signal_dir=signal_dir,
            max_stocks=args.max_stocks,
        )

    elif args.mode == 'backtest':
        result = run_backtest(
            start_date=args.start,
            end_date=args.end,
            strategy_combo=args.strategy_combo,
            config=config,
            use_vectorized=not args.no_vectorized,
        )

    elif args.mode == 'full':
        precompute_signals(
            start_date=args.start,
            end_date=args.end,
            strategy_names=strategy_names,
            signal_dir=signal_dir,
            max_stocks=args.max_stocks,
        )

        result = run_backtest(
            start_date=args.start,
            end_date=args.end,
            strategy_combo=args.strategy_combo,
            config=config,
            use_vectorized=not args.no_vectorized,
        )

    logger.info("\n完成！")


if __name__ == '__main__':
    main()