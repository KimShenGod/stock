"""
策略选股页面

提供策略选择、组合配置和选股执行功能
"""

import streamlit as st
import pandas as pd
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from dashboard.services.strategy_service import strategy_service

st.title("🎯 策略选股")
st.markdown("选择策略组合，执行选股，查看结果")

# 左右布局
col_left, col_right = st.columns([1, 2])

with col_left:
    st.subheader("策略配置")

    # 策略来源选择
    strategy_source = st.radio(
        "策略来源",
        ["strategy_registry", "CeLue"],
        help="选择使用哪个模块的策略"
    )

    # 获取策略列表
    all_strategies = strategy_service.get_strategy_names()

    st.markdown(f"**可用策略 ({len(all_strategies)}个)**")

    # 预定义组合选择
    combos = strategy_service.get_combos()
    combo_names = list(combos.keys())

    selected_combo = st.selectbox(
        "选择预定义组合",
        combo_names,
        help="从config.yml中选择策略组合"
    )

    # 显示组合中的策略
    if selected_combo and selected_combo in combos:
        combo_strategies = combos[selected_combo]
        st.info(f"组合策略: {', '.join(combo_strategies)}")

    # 组合模式
    combine_mode = st.radio(
        "组合模式",
        ["AND", "OR"],
        help="AND: 同时满足所有策略; OR: 满足任一策略"
    )

    # 自定义策略选择
    st.markdown("---")
    st.markdown("**自定义策略组合**")

    custom_strategies = st.multiselect(
        "选择策略",
        all_strategies,
        default=combos.get(selected_combo, []),
        help="手动选择多个策略"
    )

    # 执行按钮
    st.markdown("---")

    use_custom = st.checkbox("使用自定义组合", value=False)

    if st.button("🚀 执行选股", type="primary", use_container_width=True):
        with st.spinner("正在执行选股..."):
            try:
                if use_custom and custom_strategies:
                    result_df = strategy_service.execute_custom(
                        strategy_names=custom_strategies,
                        mode=combine_mode,
                    )
                elif selected_combo:
                    result_df = strategy_service.execute_combo(
                        combo_name=selected_combo,
                        mode=combine_mode,
                    )
                else:
                    st.warning("请选择策略组合或自定义策略")
                    result_df = pd.DataFrame()

                if not result_df.empty:
                    st.session_state['strategy_results'] = result_df
                    st.session_state['quote_stocks'] = result_df['代码'].tolist()
                    st.success(f"选股完成，选出 {len(result_df)} 只股票")
                else:
                    st.warning("未选出任何股票")

            except Exception as e:
                st.error(f"选股执行失败: {e}")

with col_right:
    st.subheader("选股结果")

    # 显示结果
    results = st.session_state.get('strategy_results')

    if results is not None and not results.empty:
        st.metric("选出股票数", len(results))

        display_df = results.copy()
        display_df['流通市值'] = display_df['流通市值'].apply(lambda x: f"{x:.2f}亿")

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "涨跌幅": st.column_config.NumberColumn(format="%.2f%%"),
                "换手率": st.column_config.NumberColumn(format="%.2f%%"),
                "量比": st.column_config.NumberColumn(format="%.2f"),
            }
        )

        # 导出按钮
        csv = results.to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            "📥 导出CSV",
            csv,
            file_name=f"选股结果_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )

        # 跳转按钮
        st.markdown("---")
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            st.info("👆 点击左侧导航'实时行情'查看选股结果行情")
        with col_btn2:
            st.info("👆 点击左侧导航'交易下单'进行下单操作")

    else:
        st.info("请先执行选股")

# 底部说明
st.markdown("---")
st.markdown("""
**说明**:
- 策略来自 `strategy_registry.py` 和 `CeLue.py`
- 组合配置来自 `config.yml`
- AND模式: 同时满足所有选中策略才入选
- OR模式: 满足任一策略即可入选
""")
