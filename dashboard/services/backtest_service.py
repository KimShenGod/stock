"""
回测服务模块

封装回测引擎，提供：
- 回测参数配置
- 执行回测
- 获取回测结果
"""

import sys
import os
import pandas as pd
import yaml
from pathlib import Path
from typing import Dict, Optional, Any
from dataclasses import dataclass, asdict

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@dataclass
class BacktestConfig:
    """回测参数配置"""
    initial_capital: float = 400000.0
    max_positions: int = 3
    stop_loss: float = 0.08
    take_profit: float = 0.30
    max_hold_days: int = 30
    commission_rate: float = 0.0003
    stamp_tax_rate: float = 0.001
    start_date: str = '20240101'
    end_date: str = '20241231'
    strategy_combo: str = 'default'


class BacktestService:
    """回测服务"""

    def __init__(self):
        self._config = self._load_default_config()
        self._result = None

    def _load_default_config(self) -> BacktestConfig:
        config_path = project_root / 'config.yml'
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    cfg = yaml.safe_load(f)
                    bt_cfg = cfg.get('backtest', {})
                    return BacktestConfig(
                        initial_capital=bt_cfg.get('initial_capital', 400000.0),
                        max_positions=bt_cfg.get('max_positions', 3),
                        stop_loss=bt_cfg.get('stop_loss', 0.08),
                        take_profit=bt_cfg.get('take_profit', 0.30),
                        max_hold_days=bt_cfg.get('max_hold_days', 30),
                        commission_rate=bt_cfg.get('commission_rate', 0.0003),
                        stamp_tax_rate=bt_cfg.get('stamp_tax_rate', 0.001),
                    )
            except Exception:
                pass
        return BacktestConfig()

    def get_default_config(self) -> Dict:
        return asdict(self._config)

    def run_backtest(
        self,
        start_date: str = None,
        end_date: str = None,
        strategy_combo: str = None,
        initial_capital: float = None,
        max_positions: int = None,
        stop_loss: float = None,
        take_profit: float = None,
        max_hold_days: int = None,
    ) -> Dict:
        """执行回测"""
        try:
            from backtest import quick_backtest

            result = quick_backtest(
                start_date=start_date or self._config.start_date,
                end_date=end_date or self._config.end_date,
                strategy_combo=strategy_combo or self._config.strategy_combo,
                initial_capital=initial_capital or self._config.initial_capital,
                max_positions=max_positions or self._config.max_positions,
                stop_loss=stop_loss or self._config.stop_loss,
                take_profit=take_profit or self._config.take_profit,
                max_hold_days=max_hold_days or self._config.max_hold_days,
            )

            self._result = result
            return self._format_result(result)

        except Exception as e:
            return {'error': str(e), 'success': False}

    def _format_result(self, result: Dict) -> Dict:
        """格式化回测结果"""
        formatted = {
            'success': True,
            'initial_capital': result.get('initial_capital', 0),
            'final_value': result.get('final_value', 0),
            'total_return_pct': result.get('total_return_pct', 0),
            'annual_return_pct': result.get('annual_return_pct', 0),
            'max_drawdown_pct': result.get('max_drawdown_pct', 0),
            'sharpe_ratio': result.get('sharpe_ratio', 0),
            'win_rate_pct': result.get('win_rate_pct', 0),
            'total_trades': result.get('total_trades', 0),
            'win_trades': result.get('win_trades', 0),
            'loss_trades': result.get('loss_trades', 0),
            'elapsed_time_str': result.get('elapsed_time_str', ''),
        }

        if 'portfolio_value' in result:
            formatted['portfolio_value'] = result['portfolio_value']
        if 'drawdown' in result:
            formatted['drawdown'] = result['drawdown']
        if 'daily_returns' in result:
            formatted['daily_returns'] = result['daily_returns']
        if 'trades' in result:
            df = result['trades']
            if isinstance(df, pd.DataFrame):
                formatted['trades_df'] = df

        return formatted

    def get_last_result(self) -> Optional[Dict]:
        return self._result


backtest_service = BacktestService()
