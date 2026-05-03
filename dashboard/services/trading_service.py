"""
交易服务模块

使用 miniQMT + xtquant 实现交易功能

依赖: pip install xtquant
配置: 需要miniQMT客户端运行，并提供userdata_mini路径
"""

import sys
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))


@dataclass
class AccountInfo:
    total_asset: float = 0.0
    cash: float = 0.0
    market_value: float = 0.0
    account_id: str = ""


@dataclass
class Position:
    stock_code: str
    stock_name: str = ""
    volume: int = 0
    can_use_volume: int = 0
    cost_price: float = 0.0
    market_value: float = 0.0
    profit_loss: float = 0.0
    profit_loss_ratio: float = 0.0


@dataclass
class Order:
    order_id: int
    stock_code: str
    order_type: int
    price: float
    volume: int
    traded_volume: int = 0
    status: int = 0


@dataclass
class Trade:
    order_id: int
    trade_id: int
    stock_code: str
    price: float
    volume: int
    traded_time: str = ""


class TradingService:
    """miniQMT交易服务"""

    def __init__(self):
        self.xt_trader = None
        self.account = None
        self.connected = False
        self.session_id = 123456

    def connect(self, mini_qmt_path: str, account_id: str, account_type: str = "STOCK") -> Dict[str, Any]:
        try:
            from xtquant.xttrader import XtQuantTrader
            from xtquant.xttype import StockAccount

            self.xt_trader = XtQuantTrader(mini_qmt_path, self.session_id)
            self.account = StockAccount(account_id, account_type)
            self.xt_trader.start()
            connect_result = self.xt_trader.connect()

            if connect_result == 0:
                subscribe_result = self.xt_trader.subscribe(self.account)
                if subscribe_result == 0:
                    self.connected = True
                    return {"success": True, "message": "连接成功"}
                else:
                    return {"success": False, "message": f"订阅失败: {subscribe_result}"}
            else:
                return {"success": False, "message": f"连接失败: {connect_result}"}
        except ImportError:
            return {"success": False, "message": "请安装xtquant: pip install xtquant"}
        except Exception as e:
            return {"success": False, "message": f"异常: {str(e)}"}

    def disconnect(self):
        self.connected = False
        self.xt_trader = None
        self.account = None

    def get_account_info(self) -> Optional[AccountInfo]:
        if not self.connected or not self.xt_trader:
            return None
        try:
            asset = self.xt_trader.query_stock_asset(self.account)
            if asset:
                return AccountInfo(
                    total_asset=asset.total_asset,
                    cash=asset.cash,
                    market_value=asset.market_value,
                    account_id=self.account.account_id
                )
        except Exception:
            pass
        return None

    def get_positions(self) -> List[Position]:
        if not self.connected or not self.xt_trader:
            return []
        try:
            positions = self.xt_trader.query_stock_positions(self.account)
            result = []
            for pos in positions:
                result.append(Position(
                    stock_code=pos.stock_code,
                    volume=pos.volume,
                    can_use_volume=pos.can_use_volume,
                    cost_price=pos.cost_price,
                    market_value=pos.market_value,
                    profit_loss=pos.float_profit,
                ))
            return result
        except Exception:
            return []

    def get_position(self, stock_code: str) -> Optional[Position]:
        if not self.connected or not self.xt_trader:
            return None
        try:
            pos = self.xt_trader.query_stock_position(self.account, stock_code)
            if pos:
                return Position(
                    stock_code=pos.stock_code,
                    volume=pos.volume,
                    can_use_volume=pos.can_use_volume,
                    cost_price=pos.cost_price,
                    market_value=pos.market_value
                )
        except Exception:
            pass
        return None

    def place_order(self, stock_code: str, direction: str, volume: int, price: float = 0.0, price_type: str = "LIMIT") -> Dict[str, Any]:
        if not self.connected or not self.xt_trader:
            return {"success": False, "message": "未连接"}
        try:
            from xtquant import xtconstant

            order_type = xtconstant.STOCK_BUY if direction.upper() == "BUY" else xtconstant.STOCK_SELL
            pt = xtconstant.MARKET_PRICE if price_type.upper() == "MARKET" else xtconstant.FIX_PRICE

            order_id = self.xt_trader.order_stock(
                self.account, stock_code, order_type, volume, pt, price, "dashboard", "order"
            )

            if order_id and order_id > 0:
                return {"success": True, "order_id": order_id, "message": "下单成功"}
            else:
                return {"success": False, "message": f"下单失败: {order_id}"}
        except Exception as e:
            return {"success": False, "message": f"异常: {str(e)}"}

    def cancel_order(self, order_id: int) -> Dict[str, Any]:
        if not self.connected or not self.xt_trader:
            return {"success": False, "message": "未连接"}
        try:
            result = self.xt_trader.cancel_order_stock(self.account, order_id)
            return {"success": result == 0, "message": "撤单成功" if result == 0 else "撤单失败"}
        except Exception as e:
            return {"success": False, "message": f"异常: {str(e)}"}

    def get_orders(self) -> List[Order]:
        if not self.connected or not self.xt_trader:
            return []
        try:
            orders = self.xt_trader.query_stock_orders(self.account)
            result = []
            for o in orders:
                result.append(Order(
                    order_id=o.order_id, stock_code=o.stock_code,
                    order_type=o.order_type, price=o.price,
                    volume=o.order_volume, traded_volume=o.traded_volume,
                    status=o.order_status
                ))
            return result
        except Exception:
            return []

    def get_trades(self) -> List[Trade]:
        if not self.connected or not self.xt_trader:
            return []
        try:
            trades = self.xt_trader.query_stock_trades(self.account)
            result = []
            for t in trades:
                result.append(Trade(
                    order_id=t.order_id, trade_id=getattr(t, 'traded_id', 0),
                    stock_code=t.stock_code, price=t.traded_price,
                    volume=t.traded_volume, traded_time=str(getattr(t, 'traded_time', ''))
                ))
            return result
        except Exception:
            return []


trading_service = TradingService()
