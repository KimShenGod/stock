# -*- coding: utf-8 -*-
"""
交易下单页面 - 使用 miniQMT + xtquant 实现实盘/模拟交易
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dashboard.services.trading_service import trading_service

st.title("\U0001f4b0 交易下单")
st.markdown("miniQMT + xtquant 实盘/模拟交易")

# 连接配置
st.subheader("连接配置")

col1, col2 = st.columns(2)
with col1:
    qmt_path = st.text_input(
        "miniQMT路径",
        value="D:/迅投极速交易终端/userdata_mini",
        help="路径指向miniQMT安装目录下的userdata_mini"
    )
    account_id = st.text_input("资金账号", placeholder="输入资金账号")
with col2:
    account_type = st.selectbox("账号类型", ["STOCK", "HUGANGTONG", "SHENGANGTONG"])

# 连接按钮
connect_status = st.session_state.get("trading_connected", False)
if not connect_status:
    if st.button("连接 miniQMT", type="primary", use_container_width=True):
        if qmt_path and account_id:
            result = trading_service.connect(qmt_path, account_id, account_type)
            if result["success"]:
                st.session_state["trading_connected"] = True
                st.success(result["message"])
            else:
                st.error(result["message"])
        else:
            st.warning("请填写miniQMT路径和资金账号")
else:
    st.success("已连接 miniQMT")
    if st.button("断开连接"):
        trading_service.disconnect()
        st.session_state["trading_connected"] = False
        st.rerun()

st.markdown("---")

if connect_status:
    # 账户信息
    st.subheader("账户信息")
    account_info = trading_service.get_account_info()
    if account_info:
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("总资产", f"{account_info.total_asset:,.2f}")
        with col2:
            st.metric("可用资金", f"{account_info.cash:,.2f}")
        with col3:
            st.metric("持仓市值", f"{account_info.market_value:,.2f}")
    else:
        st.info("查询账户信息失败")

    # 持仓
    st.subheader("持仓")
    positions = trading_service.get_positions()
    if positions:
        pos_data = []
        for p in positions:
            pos_data.append({
                "代码": p.stock_code,
                "持仓": p.volume,
                "可用": p.can_use_volume,
                "成本": f"{p.cost_price:.3f}",
                "市值": f"{p.market_value:.2f}",
                "浮动盈亏": f"{p.profit_loss:.2f}",
            })
        st.dataframe(pd.DataFrame(pos_data), use_container_width=True, hide_index=True)
    else:
        st.info("暂无持仓")

    # 下单面板
    st.subheader("下单")
    col1, col2, col3 = st.columns(3)
    with col1:
        # 从选股结果导入
        strategy_results = st.session_state.get("strategy_results")
        default_code = ""
        if strategy_results is not None and not strategy_results.empty:
            default_code = strategy_results.iloc[0]["代码"]
        order_code = st.text_input("股票代码", value=default_code, placeholder="000001.SZ")
        direction = st.radio("方向", ["BUY", "SELL"], horizontal=True)
    with col2:
        order_price = st.number_input("价格", value=0.0, min_value=0.0, step=0.01)
        price_type = st.selectbox("价格类型", ["LIMIT", "MARKET"])
    with col3:
        order_volume = st.number_input("数量(股)", value=100, min_value=100, step=100)
        st.markdown("")
        if st.button("确认下单", type="primary", use_container_width=True):
            if order_code:
                result = trading_service.place_order(
                    stock_code=order_code,
                    direction=direction,
                    volume=order_volume,
                    price=order_price,
                    price_type=price_type
                )
                if result["success"]:
                    st.success(f"{result['message']}, 委托号: {result.get('order_id', '')}")
                else:
                    st.error(result["message"])
            else:
                st.warning("请输入股票代码")

    # 委托记录
    st.subheader("委托记录")
    orders = trading_service.get_orders()
    if orders:
        order_data = []
        for o in orders:
            order_data.append({
                "委托号": o.order_id,
                "代码": o.stock_code,
                "方向": "买入" if o.order_type == 23 else "卖出",
                "价格": o.price,
                "数量": o.volume,
                "已成交": o.traded_volume,
                "状态": str(o.status),
            })
        order_df = pd.DataFrame(order_data)
        st.dataframe(order_df, use_container_width=True, hide_index=True)

        # 撤单
        if orders:
            selected_order = st.selectbox(
                "选择委托撤单",
                [str(o.order_id) for o in orders],
                format_func=lambda x: f"委托 {x}"
            )
            if st.button("撤单"):
                result = trading_service.cancel_order(int(selected_order))
                if result["success"]:
                    st.success(result["message"])
                else:
                    st.error(result["message"])
    else:
        st.info("暂无委托")

    # 成交记录
    st.subheader("成交记录")
    trades = trading_service.get_trades()
    if trades:
        trade_data = []
        for t in trades:
            trade_data.append({
                "委托号": t.order_id,
                "股票": t.stock_code,
                "成交价": t.price,
                "成交量": t.volume,
                "时间": t.traded_time,
            })
        st.dataframe(pd.DataFrame(trade_data), use_container_width=True, hide_index=True)
    else:
        st.info("暂无成交")
else:
    st.info("请先连接miniQMT交易平台")

st.markdown("---")
st.markdown("""
**使用说明**:
- 需要先安装xtquant: `pip install xtquant`
- 需要启动miniQMT客户端（国信iQuant/QMT均可）
- 路径指向miniQMT安装目录下的 `userdata_mini`
- 股票代码格式: `000001.SZ` / `600000.SH`
- 100股为最小交易单位
""")
