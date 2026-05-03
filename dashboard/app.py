"""
Streamlit Dashboard 主入口

股票分析系统的实时监控面板，包含：
- 策略选股：选择策略组合并执行选股
- 回测分析：设置参数运行回测，查看结果
- 实时行情：监控选股结果的实时行情
- 交易下单：iQuant/QMT实盘或模拟交易
"""

import streamlit as st
from pathlib import Path

# 页面配置
st.set_page_config(
    page_title="股票分析Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 初始化session_state
if 'selected_strategies' not in st.session_state:
    st.session_state['selected_strategies'] = []
if 'strategy_results' not in st.session_state:
    st.session_state['strategy_results'] = None
if 'backtest_result' not in st.session_state:
    st.session_state['backtest_result'] = None
if 'quote_stocks' not in st.session_state:
    st.session_state['quote_stocks'] = []
if 'trading_connected' not in st.session_state:
    st.session_state['trading_connected'] = False

# 侧边栏标题
st.sidebar.title("📈 股票分析Dashboard")
st.sidebar.markdown("---")

# 页面导航
page = st.sidebar.radio(
    "功能导航",
    ["策略选股", "回测分析", "实时行情", "交易下单"],
    label_visibility="collapsed"
)

st.sidebar.markdown("---")
st.sidebar.markdown("### 状态信息")

# 显示当前状态
if st.session_state['strategy_results'] is not None:
    st.sidebar.success(f"选股结果: {len(st.session_state['strategy_results'])}只")

if st.session_state['backtest_result'] is not None:
    result = st.session_state['backtest_result']
    if 'total_return_pct' in result:
        color = "success" if result['total_return_pct'] > 0 else "error"
        getattr(st.sidebar, color)(f"回测收益: {result['total_return_pct']:.2f}%")

if st.session_state['trading_connected']:
    st.sidebar.success("交易: 已连接")
else:
    st.sidebar.info("交易: 未连接")

# 根据选择加载页面
pages_dir = Path(__file__).parent / "pages"

if page == "策略选股":
    import importlib.util
    spec = importlib.util.spec_from_file_location("strategy_page", pages_dir / "01_策略选股.py")
    strategy_page = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy_page)
elif page == "回测分析":
    import importlib.util
    spec = importlib.util.spec_from_file_location("backtest_page", pages_dir / "02_回测分析.py")
    backtest_page = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(backtest_page)
elif page == "实时行情":
    import importlib.util
    spec = importlib.util.spec_from_file_location("quote_page", pages_dir / "03_实时行情.py")
    quote_page = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(quote_page)
elif page == "交易下单":
    import importlib.util
    spec = importlib.util.spec_from_file_location("trading_page", pages_dir / "04_交易下单.py")
    trading_page = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(trading_page)