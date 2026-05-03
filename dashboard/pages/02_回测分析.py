"""
回测分析页面

提供回测参数设置、执行回测、查看结果功能
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dashboard.services.backtest_service import backtest_service
from dashboard.services.strategy_service import strategy_service

st.title("📊 回测分析")
st.markdown("设置回测参数，运行回测，查看收益曲线和交易记录")

# 参数表单（可折叠）
with st.expander("⚙️ 回测参数设置", expanded=True):
    col1, col2, col3 = st.columns(3)

    with col1:
        start_date = st.date_input(
            "开始日期",
            value=datetime(2024, 1, 1),
            help="回测起始日期"
        )
        initial_capital = st.number_input(
            "初始资金",
            value=400000,
            min_value=10000,
            max_value=10000000,
            step=10000,
            help="初始投入资金"
        )
        max_positions = st.slider(
            "最大持仓数",
            min_value=1,
            max_value=10,
            value=3,
            help="同时持有的最大股票数量"
        )

    with col2:
        end_date = st.date_input(
            "结束日期",
            value=datetime(2024, 12, 31),
            help="回测结束日期"
        )
        stop_loss = st.slider(
            "止损比例 (%)",
            min_value=0,
            max_value=20,
            value=8,
            help="触发止损的下跌比例"
        )
        take_profit = st.slider(
            "止盈比例 (%)",
            min_value=0,
            max_value=50,
            value=30,
            help="触发止盈的上涨比例"
        )

    with col3:
        combos = strategy_service.get_combos()
        combo_names = list(combos.keys())
        combo_display = {name: f"{name} ({', '.join(combos[name])})" for name in combo_names}
        selected_combo = st.selectbox(
            "策略组合",
            combo_names,
            format_func=lambda x: combo_display.get(x, x),
            help="选择策略组合"
        )
        max_hold_days = st.slider(
            "最大持仓天数",
            min_value=1,
            max_value=60,
            value=30,
            help="持仓超过此天数强制卖出"
        )

    # 执行按钮
    st.markdown("---")
    run_btn = st.button("🚀 运行回测", type="primary", use_container_width=True)

# 执行回测
if run_btn:
    with st.spinner("正在运行回测..."):
        result = backtest_service.run_backtest(
            start_date=start_date.strftime('%Y%m%d'),
            end_date=end_date.strftime('%Y%m%d'),
            strategy_combo=selected_combo,
            initial_capital=initial_capital,
            max_positions=max_positions,
            stop_loss=stop_loss / 100,
            take_profit=take_profit / 100,
            max_hold_days=max_hold_days,
        )

        st.session_state['backtest_result'] = result

        if result.get('success'):
            st.success(f"回测完成，用时 {result.get('elapsed_time_str', '')}")
        else:
            st.error(f"回测失败: {result.get('error', '未知错误')}")

# 显示结果
result = st.session_state.get('backtest_result')

if result and result.get('success'):
    # 指标卡片
    st.subheader("回测指标")
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric(
            "总收益率",
            f"{result['total_return_pct']:.2f}%",
            delta=f"{result['total_return_pct']:.2f}%"
        )
    with col2:
        st.metric(
            "年化收益",
            f"{result['annual_return_pct']:.2f}%"
        )
    with col3:
        st.metric(
            "最大回撤",
            f"{result['max_drawdown_pct']:.2f}%",
            delta=-result['max_drawdown_pct']
        )
    with col4:
        st.metric(
            "夏普比率",
            f"{result['sharpe_ratio']:.2f}"
        )
    with col5:
        st.metric(
            "胜率",
            f"{result['win_rate_pct']:.1f}%"
        )

    # 净值曲线图
    st.subheader("收益曲线")

    if 'portfolio_value' in result:
        portfolio_value = result['portfolio_value']

        fig = make_subplots(
            rows=2, cols=1,
            shared_xaxes=True,
            vertical_spacing=0.03,
            row_heights=[0.7, 0.3],
            subplot_titles=('净值曲线', '回撤曲线')
        )

        # 净值曲线
        fig.add_trace(
            go.Scatter(
                x=portfolio_value.index,
                y=portfolio_value.values,
                mode='lines',
                name='净值',
                line=dict(color='#1f77b4', width=2),
            ),
            row=1, col=1
        )

        # 回撤曲线
        if 'drawdown' in result:
            drawdown = result['drawdown']
            fig.add_trace(
                go.Scatter(
                    x=drawdown.index,
                    y=drawdown.values * 100,
                    mode='lines',
                    name='回撤',
                    line=dict(color='#d62728', width=1),
                    fill='tozeroy',
                    fillcolor='rgba(214, 39, 40, 0.3)',
                ),
                row=2, col=1
            )

        fig.update_layout(
            height=500,
            showlegend=True,
            xaxis_rangeslider_visible=False,
        )

        fig.update_xaxes(title_text="日期", row=2, col=1)
        fig.update_yaxes(title_text="净值", row=1, col=1)
        fig.update_yaxes(title_text="回撤%", row=2, col=1)

        st.plotly_chart(fig, use_container_width=True)

    # 交易统计
    st.subheader("交易统计")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("总交易次数", result['total_trades'])
    with col2:
        st.metric("盈利次数", result['win_trades'])
    with col3:
        st.metric("亏损次数", result['loss_trades'])

    # 交易记录
    st.subheader("交易记录")
    if 'trades_df' in result:
        trades_df = result['trades_df']
        st.dataframe(trades_df, use_container_width=True, hide_index=True)
    else:
        st.info("无交易记录")

elif result and not result.get('success'):
    st.error(f"回测失败: {result.get('error', '未知错误')}")

else:
    st.info("请设置参数并运行回测")
