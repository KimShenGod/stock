"""
实时行情页面 - 提供选股结果的实时行情监控，支持5秒自动刷新
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import time
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dashboard.services.quote_service import quote_service, get_realtime_quotes, get_intraday

st.title("📈 实时行情")
st.markdown("选股结果实时监控，每5秒自动刷新行情数据")
st.subheader("监控列表")

tab1, tab2 = st.tabs(["从选股结果导入", "手动输入"])

with tab1:
    strategy_results = st.session_state.get("strategy_results")
    if strategy_results is not None and not strategy_results.empty:
        stocks = strategy_results["代码"].tolist()
        st.success(f"已有 {len(stocks)} 只选股结果")
        if st.button("使用选股结果", use_container_width=True):
            st.session_state["quote_stocks"] = stocks
    else:
        st.info("暂无选股结果，请先在策略选股页面执行选股")

with tab2:
    manual = st.text_area("输入股票代码", placeholder="000001", height=100)
    if st.button("加载"):
        if manual:
            codes = [c.strip() for c in manual.split(chr(10)) if c.strip()]
            st.session_state["quote_stocks"] = codes

current_stocks = st.session_state.get("quote_stocks", [])

if current_stocks:
    st.metric("监控数", len(current_stocks))

    try:
        from streamlit_autorefresh import st_autorefresh
        st_autorefresh(interval=5000, key="quote_refresh", limit=None)
    except ImportError:
        st.warning("pip install streamlit-autorefresh")

    quotes_df = get_realtime_quotes(current_stocks)

    if quotes_df is not None and not quotes_df.empty:
        st.subheader("基础行情")
        cols = ["代码", "现价", "涨跌幅", "涨跌额", "开盘", "最高", "最低", "成交量", "成交额"]
        avail = [c for c in cols if c in quotes_df.columns]
        st.dataframe(quotes_df[avail], use_container_width=True, hide_index=True)

        stock = st.selectbox("详情", quotes_df["代码"].tolist() if "代码" in quotes_df.columns else [])
        if stock:
            row = quotes_df[quotes_df["代码"] == stock].iloc[0]
            price_val = row["现价"] if "现价" in row.index else "N/A"
            st.markdown(f"**{stock} 现价: {price_val}**")
            intraday = get_intraday(stock)
            if intraday is not None and not intraday.empty:
                fig = go.Figure()
                price_col = "price" if "price" in intraday.columns else "现价" if "现价" in intraday.columns else None
                if price_col:
                    fig.add_trace(go.Scatter(y=intraday[price_col], mode="lines", name="分时"))
                st.plotly_chart(fig, use_container_width=True)

    st.caption(f"更新: {time.strftime('%H:%M:%S')}")
else:
    st.info("请添加股票代码")
