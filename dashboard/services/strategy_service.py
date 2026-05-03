"""
策略服务模块

封装选股策略逻辑，提供：
- 获取可用策略列表
- 获取预定义策略组合
- 执行选股策略
"""

import sys
import os
import pandas as pd
import yaml
from pathlib import Path
from typing import List, Dict, Optional, Any

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class StrategyService:
    """选股策略服务"""

    def __init__(self):
        self._strategies = {}
        self._combos = {}
        self._load_strategies()
        self._load_config()

    def _load_strategies(self):
        try:
            from strategy_registry import list_strategies, get_strategy
            for name in list_strategies():
                self._strategies[name] = {
                    'name': name,
                    'func': get_strategy(name),
                    'source': 'strategy_registry'
                }
        except ImportError:
            pass

        try:
            import CeLue
            if hasattr(CeLue, 'strategy_list'):
                for item in CeLue.strategy_list:
                    name = item.get('name', item.get('策略名', ''))
                    if name and name not in self._strategies:
                        self._strategies[name] = {
                            'name': name,
                            'func': item.get('func'),
                            'source': 'CeLue'
                        }
        except ImportError:
            pass

    def _load_config(self):
        config_path = project_root / 'config.yml'
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    self._combos = config.get('strategies', {})
            except Exception as e:
                print(f"加载配置文件失败: {e}")

    def get_strategies(self) -> List[Dict]:
        return [
            {'name': name, 'source': info['source']}
            for name, info in self._strategies.items()
        ]

    def get_strategy_names(self) -> List[str]:
        return list(self._strategies.keys())

    def get_combos(self) -> Dict[str, List[str]]:
        return self._combos

    def get_combo_names(self) -> List[str]:
        return list(self._combos.keys())

    def execute_combo(
        self,
        combo_name: str,
        mode: str = 'AND',
        stock_list: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        if combo_name not in self._combos:
            return pd.DataFrame()

        strategy_names = self._combos[combo_name]
        return self.execute_custom(strategy_names, mode, stock_list)

    def execute_custom(
        self,
        strategy_names: List[str],
        mode: str = 'AND',
        stock_list: Optional[List[str]] = None,
    ) -> pd.DataFrame:
        if stock_list is None:
            stock_list = self._get_all_stocks()

        all_results = {}
        stock_data_cache = {}
        total_strategies = len(strategy_names)

        for name in strategy_names:
            if name not in self._strategies:
                continue
            strategy_func = self._strategies[name]['func']
            if strategy_func is None:
                continue

            for stock in stock_list:
                try:
                    df = self._load_stock_data(stock)
                    if df is None or df.empty:
                        continue
                    if stock not in stock_data_cache:
                        stock_data_cache[stock] = df
                    signal = strategy_func(df)
                    if signal is not None and len(signal) > 0:
                        last_signal = signal.iloc[0] if hasattr(signal, 'iloc') else signal
                        if last_signal:
                            if stock not in all_results:
                                all_results[stock] = []
                            all_results[stock].append(name)
                except Exception as e:
                    print(f"策略 {name} 对 {stock} 失败: {e}")

        results = []
        for stock, triggered in all_results.items():
            if mode == 'AND' and len(triggered) != total_strategies:
                continue

            df = stock_data_cache.get(stock)
            row = df.iloc[0] if df is not None and not df.empty else None

            stock_name = row['name'] if row is not None and 'name' in row.index else ''
            close_price = row['close'] if row is not None else 0
            prev_close = df.iloc[1]['close'] if df is not None and len(df) > 1 else close_price
            change_pct = round((close_price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0
            turnover = row['换手率'] if row is not None and '换手率' in row.index else 0
            vol_ratio = row['量比'] if row is not None and '量比' in row.index else 0
            market_cap = row['流通市值'] if row is not None and '流通市值' in row.index else 0

            results.append({
                '代码': stock,
                '证券名称': stock_name,
                '最新价': round(close_price, 2),
                '涨跌幅': change_pct,
                '换手率': round(float(turnover), 2) if pd.notna(turnover) else 0,
                '量比': round(float(vol_ratio), 2) if pd.notna(vol_ratio) else 0,
                '流通市值': round(float(market_cap) / 1e8, 2) if pd.notna(market_cap) else 0,
            })

        df = pd.DataFrame(results)
        if not df.empty:
            df = df.sort_values('涨跌幅', ascending=False).reset_index(drop=True)
        return df

    def _get_all_stocks(self) -> List[str]:
        try:
            import user_config as ucfg
            csv_dir = ucfg.tdx.get('csv_lday', '')
            if csv_dir and os.path.exists(csv_dir):
                return [f[:-4] for f in os.listdir(csv_dir) if f.endswith('.csv')]
        except Exception:
            pass
        return []

    def _load_stock_data(self, stock_code: str) -> Optional[pd.DataFrame]:
        try:
            import user_config as ucfg
            pickle_dir = ucfg.tdx.get('pickle', '')
            if pickle_dir and os.path.exists(pickle_dir):
                file_path = os.path.join(pickle_dir, f"{stock_code}.pkl")
                if os.path.exists(file_path):
                    return pd.read_pickle(file_path)
        except Exception:
            pass
        return None


strategy_service = StrategyService()
