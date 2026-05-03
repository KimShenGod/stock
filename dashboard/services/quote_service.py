"""
行情服务模块

封装pytdx和东方财富行情获取接口，提供：
- 基础实时行情（当前价、涨跌幅、成交量等）
- 五档盘口数据（买一~买五、卖一~卖五）
- 分时走势数据
- 技术指标计算（换手率、量比、振幅）
"""

import sys
import os
import time
import pandas as pd
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Any

# 添加项目根目录到路径
project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


class QuoteService:
    """实时行情服务"""

    _instance = None
    _api = None
    _connected = False
    _best_ip = '180.153.18.170'
    _best_port = 7709

    FALLBACK_SERVERS = [
        ('180.153.18.170', 7709),
        ('180.153.39.51', 7709),
        ('14.215.128.18', 7709),
        ('59.173.18.140', 7709),
        ('180.153.18.17', 7709),
        ('123.125.108.24', 7709),
    ]

    def __new__(cls):
        """单例模式，保持连接池"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """初始化行情服务"""
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._load_config()

    def _load_config(self):
        """加载服务器配置"""
        try:
            import user_config as ucfg
            self._best_ip = ucfg.tdx.get('pytdx_ip', self._best_ip)
            self._best_port = ucfg.tdx.get('pytdx_port', self._best_port)
        except ImportError:
            pass

    def _connect(self) -> bool:
        """连接pytdx服务器，支持多服务器回退"""
        if self._connected and self._api is not None:
            return True

        try:
            from pytdx.hq import TdxHq_API
            self._api = TdxHq_API()

            # 尝试自动选择最优服务器
            try:
                from pytdx_best_ip import custom_select_best_ip
                best_info = custom_select_best_ip()
                ip, port = best_info['ip'], best_info['port']
                if self._api.connect(ip, port):
                    self._best_ip, self._best_port = ip, port
                    self._connected = True
                    return True
            except Exception:
                pass

            # 尝试配置文件中的服务器
            if self._api.connect(self._best_ip, self._best_port):
                self._connected = True
                return True

            # 回退到备用服务器列表
            for ip, port in self.FALLBACK_SERVERS:
                try:
                    if self._api.connect(ip, port):
                        self._best_ip, self._best_port = ip, port
                        self._connected = True
                        return True
                except Exception:
                    continue

            return False
        except Exception as e:
            print(f"pytdx连接失败: {e}")
            return False

    def _disconnect(self):
        """断开连接"""
        if self._api and self._connected:
            try:
                self._api.disconnect()
            except Exception:
                pass
            self._connected = False

    def get_quotes(self, stocklist: List[str]) -> pd.DataFrame:
        """
        获取实时行情

        Args:
            stocklist: 股票代码列表，如 ['000001', '600030']

        Returns:
            DataFrame包含：代码, 名称, 现价, 开盘, 最高, 最低, 成交量, 成交额,
                          涨跌幅, 涨跌额, 买一价, 卖一价, 买一量, 卖一量等
        """
        if not stocklist:
            return pd.DataFrame()

        # 转换为pytdx格式 (market, code)
        stocklist_pytdx = []
        for code in stocklist:
            if isinstance(code, str):
                market = 1 if code.startswith('6') else 0
                stocklist_pytdx.append((market, code))

        df = pd.DataFrame()

        if not self._connect():
            return df

        try:
            # 分批获取，每批最多80只
            batch_size = 80
            for i in range(0, len(stocklist_pytdx), batch_size):
                batch = stocklist_pytdx[i:i+batch_size]
                try:
                    data = self._api.to_df(self._api.get_security_quotes(batch))
                    if not data.empty:
                        df = pd.concat([df, data], ignore_index=True)
                except Exception as e:
                    print(f"获取批次行情失败: {e}")

            if not df.empty:
                df = df[df['code'].notna()].reset_index(drop=True)

                # 计算涨跌幅和涨跌额
                if 'price' in df.columns and 'last_close' in df.columns:
                    df['change_pct'] = (df['price'] - df['last_close']) / df['last_close'] * 100
                    df['change_amt'] = df['price'] - df['last_close']

                # 格式化列名
                column_map = {
                    'code': '代码',
                    'name': '名称',
                    'price': '现价',
                    'open': '开盘',
                    'high': '最高',
                    'low': '最低',
                    'vol': '成交量',
                    'amount': '成交额',
                    'change_pct': '涨跌幅',
                    'change_amt': '涨跌额',
                    'bid1': '买一价', 'bid2': '买二价', 'bid3': '买三价',
                    'bid4': '买四价', 'bid5': '买五价',
                    'ask1': '卖一价', 'ask2': '卖二价', 'ask3': '卖三价',
                    'ask4': '卖四价', 'ask5': '卖五价',
                    'bid_vol1': '买一量', 'bid_vol2': '买二量', 'bid_vol3': '买三量',
                    'bid_vol4': '买四量', 'bid_vol5': '买五量',
                    'ask_vol1': '卖一量', 'ask_vol2': '卖二量', 'ask_vol3': '卖三量',
                    'ask_vol4': '卖四量', 'ask_vol5': '卖五量',
                }
                df = df.rename(columns=column_map)

        except Exception as e:
            print(f"获取行情失败: {e}")

        return df

    def get_five_level_quotes(self, stocklist: List[str]) -> pd.DataFrame:
        """
        获取五档盘口数据

        Args:
            stocklist: 股票代码列表

        Returns:
            DataFrame包含五档买卖价量
        """
        df = self.get_quotes(stocklist)

        if df.empty:
            return df

        # 提取五档列
        five_level_cols = ['代码', '名称', '现价']
        for i in range(1, 6):
            for prefix in ['买', '卖']:
                five_level_cols.extend([f'{prefix}{i}价', f'{prefix}{i}量'])

        available_cols = [c for c in five_level_cols if c in df.columns]
        return df[available_cols]

    def get_intraday_data(self, stock_code: str, market: str = None) -> pd.DataFrame:
        """
        获取分时走势数据

        Args:
            stock_code: 股票代码
            market: 市场代码，None自动判断

        Returns:
            DataFrame包含：time, price, volume, avg_price等
        """
        try:
            sys.path.insert(0, str(project_root))
            from get_eastmoney_enhanced import get_intraday_data

            df = get_intraday_data(stock_code, market=market, ndays=1)
            return df
        except Exception as e:
            print(f"获取分时数据失败: {e}")
            return pd.DataFrame()

    def calculate_technical_indicators(
        self,
        quotes_df: pd.DataFrame,
        additional_data: Optional[Dict[str, Any]] = None
    ) -> pd.DataFrame:
        """
        计算技术指标

        Args:
            quotes_df: 行情DataFrame
            additional_data: 补充数据（流通市值、历史成交量等）

        Returns:
            添加技术指标列的DataFrame
        """
        if quotes_df.empty:
            return quotes_df

        df = quotes_df.copy()

        # 振幅 = (最高-最低)/现价 * 100
        if all(c in df.columns for c in ['最高', '最低', '现价']):
            valid = df['现价'] > 0
            df.loc[valid, '振幅'] = (df.loc[valid, '最高'] - df.loc[valid, '最低']) / df.loc[valid, '现价'] * 100

        # 换手率需要流通市值数据
        if additional_data and '流通市值' in additional_data:
            float_cap = additional_data['流通市值']
            if '成交额' in df.columns and float_cap > 0:
                df['换手率'] = df['成交额'] / float_cap * 100

        # 量比需要历史平均成交量
        if additional_data and 'avg_volume_5d' in additional_data:
            avg_vol = additional_data['avg_volume_5d']
            if '成交量' in df.columns and avg_vol > 0:
                df['量比'] = df['成交量'] / avg_vol

        return df

    def is_trading_time(self) -> bool:
        """判断是否在交易时段"""
        now = time.localtime()
        hour = now.tm_hour
        minute = now.tm_min

        # 上午交易时段 9:30-11:30
        if hour == 9 and minute >= 30:
            return True
        if hour == 10:
            return True
        if hour == 11 and minute <= 30:
            return True

        # 下午交易时段 13:00-15:00
        if hour == 13:
            return True
        if hour == 14:
            return True
        if hour == 15 and minute == 0:
            return True

        return False

    def get_stock_list_from_local(self) -> List[str]:
        """从本地数据目录获取股票列表"""
        try:
            import user_config as ucfg
            csv_dir = ucfg.tdx.get('csv_lday', '')
            if csv_dir and os.path.exists(csv_dir):
                stocks = []
                for f in os.listdir(csv_dir):
                    if f.endswith('.csv'):
                        stocks.append(f[:-4])
                return stocks
        except Exception:
            pass
        return []


# 全局实例
quote_service = QuoteService()


def get_realtime_quotes(stocklist: List[str]) -> pd.DataFrame:
    """快捷接口：获取实时行情"""
    return quote_service.get_quotes(stocklist)


def get_intraday(stock_code: str) -> pd.DataFrame:
    """快捷接口：获取分时数据"""
    return quote_service.get_intraday_data(stock_code)