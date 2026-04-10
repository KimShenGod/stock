"""
VectorBT 快速回测脚本 v6
支持装饰器策略注册 + 配置文件策略组合
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import numpy as np
import pandas as pd
import vectorbt as vbt
import time
import yaml
from tqdm import tqdm
from numba import jit

# ============== 回测配置 ==============
start_date = "2024-01-01"
end_date = "2026-03-31"
initial_cash = 400000

# 止损止盈
STOP_LOSS = 0.08     # 止损 8%
TAKE_PROFIT = 0.30   # 止盈 30%
MAX_HOLD_DAYS = 30   # 最大持仓天数
MAX_POSITIONS = 3    # 最大持仓数

# 策略组合配置（从配置文件读取）
STRATEGY_COMBO_NAME = "default"  # 可选: default, combo_1, combo_2, combo_3

# 数据路径
import user_config as ucfg
csvdaypath = ucfg.tdx['pickle']

# 导入策略注册表
from strategy_registry import get_strategy, list_strategies


@jit(nopython=True)
def calculate_exit_signals_numba(close_prices, entry_signals, 
                                   stop_loss, take_profit, max_hold_days,
                                   max_positions):
    """
    使用 Numba 加速计算止损止盈退出信号（动态多持仓模式）
    """
    n_rows, n_cols = close_prices.shape
    exit_signals = np.zeros((n_rows, n_cols), dtype=np.bool_)
    filtered_entries = np.zeros((n_rows, n_cols), dtype=np.bool_)
    
    # 跟踪持仓状态
    entry_indices = np.full(n_cols, -1, dtype=np.int64)
    current_positions = 0
    
    # 当天有信号的股票列表
    daily_candidates = []
    
    for i in range(n_rows):
        # 第一步：检查持仓退出
        for j in range(n_cols):
            if entry_indices[j] >= 0:
                entry_idx = entry_indices[j]
                entry_price = close_prices[entry_idx, j]
                current_price = close_prices[i, j]
                hold_days = i - entry_idx
                
                pnl_pct = (current_price - entry_price) / entry_price
                
                should_exit = False
                if pnl_pct <= -stop_loss or pnl_pct >= take_profit or hold_days >= max_hold_days:
                    should_exit = True
                
                if should_exit:
                    exit_signals[i, j] = True
                    entry_indices[j] = -1
                    current_positions -= 1
        
        # 第二步：收集当天有信号的股票
        daily_candidates.clear()
        for j in range(n_cols):
            if entry_signals[i, j] and entry_indices[j] == -1:
                daily_candidates.append(j)
        
        # 第三步：买入（最多填满空位）
        available_slots = max_positions - current_positions
        buy_count = min(available_slots, len(daily_candidates))
        
        for k in range(buy_count):
            j = daily_candidates[k]
            entry_indices[j] = i
            filtered_entries[i, j] = True
            current_positions += 1
    
    return exit_signals, filtered_entries


def load_all_stock_data():
    """加载所有股票数据"""
    print("="*60)
    print("加载股票数据...")
    print("="*60)
    
    stocklist = [i[:-4] for i in os.listdir(csvdaypath) if i.endswith('.pkl')]
    stocklist = [s for s in stocklist if s[:2] != '68']  # 剔除科创板
    
    print(f"发现 {len(stocklist)} 只股票")
    
    close_data = {}
    open_data = {}
    high_data = {}      # 新增：最高价数据
    circ_mv_data = {}
    
    for stockcode in tqdm(stocklist, desc="加载数据"):
        try:
            df = pd.read_pickle(os.path.join(csvdaypath, stockcode + '.pkl'))
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')
            
            # 筛选日期范围
            df = df[(df['date'] >= start_date) & (df['date'] <= end_date)]
            
            if len(df) < 10:
                continue
            
            date_index = pd.DatetimeIndex(df['date']).tz_localize(None)
            
            close_data[stockcode] = pd.Series(df['close'].values, index=date_index)
            open_data[stockcode] = pd.Series(df['open'].values, index=date_index)
            high_data[stockcode] = pd.Series(df['high'].values, index=date_index)
            
            if '流通市值' in df.columns:
                circ_mv_data[stockcode] = pd.Series(df['流通市值'].values, index=date_index)
            else:
                circ_mv_data[stockcode] = pd.Series(np.inf, index=date_index)
                
        except Exception:
            continue
    
    close_df = pd.DataFrame(close_data)
    open_df = pd.DataFrame(open_data)
    high_df = pd.DataFrame(high_data)
    circ_mv_df = pd.DataFrame(circ_mv_data)
    
    print(f"成功加载 {len(close_df.columns)} 只股票")
    print(f"数据范围: {close_df.index[0]} ~ {close_df.index[-1]}")
    print(f"共 {len(close_df)} 个交易日")
    
    return close_df, open_df, high_df, circ_mv_df


def calculate_signals(close_df, open_df, high_df, circ_mv_df):
    """计算选股信号（从配置文件读取策略组合）"""
    print("\n计算选股信号...")
    
    # 读取配置文件中的策略组合
    config_path = os.path.join(os.path.dirname(__file__), 'config.yml')
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    strategy_names = config['strategies'].get(STRATEGY_COMBO_NAME, ['高开涨停', '小市值'])
    print(f"使用策略组合: {strategy_names}")
    
    # 获取所有可用策略
    available = list_strategies()
    print(f"可用策略: {available}")
    
    # 检查策略是否都存在
    missing = [s for s in strategy_names if s not in available]
    if missing:
        print(f"警告: 策略 {missing} 不存在，将被忽略")
    
    # 为每只股票计算各策略信号，然后组合
    strategy_signals = {}
    
    for stock_code in close_df.columns:
        try:
            # 构建单只股票的 DataFrame
            stock_df = pd.DataFrame({
                'code': stock_code,
                'open': open_df[stock_code],
                'high': high_df[stock_code],
                'low': close_df[stock_code],  # 用收盘价近似最低价
                'close': close_df[stock_code],
            })
            
            # 添加流通市值（如果存在）
            if stock_code in circ_mv_df.columns:
                stock_df['流通市值'] = circ_mv_df[stock_code]
            
            stock_df.index = close_df.index
            
            # 对每个策略计算信号
            stock_signals = []
            for name in strategy_names:
                if name not in available:
                    continue
                func = get_strategy(name)
                if func:
                    signal = func(stock_df)
                    if isinstance(signal, pd.Series):
                        stock_signals.append(signal)
            
            # AND 组合所有策略信号
            if stock_signals:
                combined = stock_signals[0]
                for s in stock_signals[1:]:
                    combined = combined & s
                strategy_signals[stock_code] = combined
            
        except Exception as e:
            continue
    
    # 合并为 DataFrame
    if strategy_signals:
        combined_signal = pd.DataFrame(strategy_signals)
        # 填充缺失值为 False
        combined_signal = combined_signal.fillna(False)
        # 确保所有列都存在
        for col in close_df.columns:
            if col not in combined_signal.columns:
                combined_signal[col] = False
        combined_signal = combined_signal[close_df.columns]
    else:
        combined_signal = pd.DataFrame(False, index=close_df.index, columns=close_df.columns)
    
    print(f"选股信号数量: {combined_signal.sum().sum()}")
    
    # 按日期统计
    daily_signal_counts = combined_signal.sum(axis=1)
    print(f"日均信号数: {daily_signal_counts.mean():.2f}")
    print(f"有信号的交易日: {(daily_signal_counts > 0).sum()}")
    
    return combined_signal


def calculate_exit_signals(close_df, entries):
    """计算止损止盈退出信号"""
    print(f"计算止损止盈退出信号")
    print(f"  止损: -{STOP_LOSS*100:.0f}%")
    print(f"  止盈: +{TAKE_PROFIT*100:.0f}%")
    print(f"  最大持仓天数: {MAX_HOLD_DAYS}")
    print(f"  最大持仓数: {MAX_POSITIONS}")
    
    close_prices = close_df.fillna(0).values
    entry_arr = entries.fillna(False).values.astype(np.bool_)
    
    exit_arr, filtered_entries = calculate_exit_signals_numba(
        close_prices,
        entry_arr,
        STOP_LOSS,
        TAKE_PROFIT,
        MAX_HOLD_DAYS,
        MAX_POSITIONS
    )
    
    exits = pd.DataFrame(exit_arr, index=close_df.index, columns=close_df.columns)
    filtered_entries_df = pd.DataFrame(filtered_entries, index=close_df.index, columns=close_df.columns)
    
    print(f"  原始入场信号: {entries.sum().sum()}")
    print(f"  过滤后入场信号: {filtered_entries_df.sum().sum()}")
    print(f"  退出信号数量: {exits.sum().sum()}")
    
    return exits, filtered_entries_df


def run_backtest(close_df, open_df, entries, exits):
    """运行 VectorBT 回测"""
    print("\n" + "="*60)
    print("运行 VectorBT 回测")
    print("="*60)
    
    entry_cols = entries.columns[entries.any(axis=0)]
    n_cols = len(entry_cols)
    
    if n_cols == 0:
        print("没有入场信号，无法回测！")
        return None
    
    print(f"有信号的股票数: {n_cols}")
    print(f"初始资金: {initial_cash:,}")
    
    close_subset = close_df[entry_cols]
    open_subset = open_df[entry_cols]
    entries_subset = entries[entry_cols]
    exits_subset = exits[entry_cols]
    
    buy_fees = 0.0003 + 0.00001
    sell_fees = 0.0003 + 0.00001 + 0.001
    cash_per_col = initial_cash / n_cols
    
    # 修复：指定频率
    try:
        freq = pd.infer_freq(close_subset.index)
        if freq is None:
            freq = 'D'
    except:
        freq = 'D'
    
    pf = vbt.Portfolio.from_signals(
        close=close_subset,
        open=open_subset,
        entries=entries_subset,
        exits=exits_subset,
        init_cash=cash_per_col,
        fees=buy_fees,
        fixed_fees=sell_fees,
        slippage=0.001,
        size=1.0,
        size_type='percent',
        accumulate=False,
        group_by=True,
        freq=freq,
        direction='longonly',
    )
    
    return pf


def print_stats(pf, initial_cash):
    """打印回测统计（手动计算指标，避免频率问题）"""
    import numpy as np
    
    trades = pf.trades
    
    if len(trades.records) == 0:
        print("\n没有交易记录！")
        return
    
    records = trades.records
    final_value = pf.value().iloc[-1] if hasattr(pf.value(), 'iloc') else pf.value()
    
    # 总收益率
    total_return = (final_value - initial_cash) / initial_cash
    
    # 年化收益（手动计算）
    values = pf.value()
    days = len(values)
    years = days / 252
    annual_return = (1 + total_return) ** (1 / years) - 1 if years > 0 else 0
    
    # 最大回撤
    cummax = values.cummax()
    drawdown = (values - cummax) / cummax
    max_drawdown = drawdown.min()
    
    # 夏普比率（手动计算）
    returns = pf.returns().dropna()
    if len(returns) > 0 and returns.std() > 0:
        sharpe = returns.mean() / returns.std() * np.sqrt(252)
    else:
        sharpe = 0
    
    print("\n" + "="*60)
    print("回测统计")
    print("="*60)
    print(f"初始资金: {initial_cash:,.2f}")
    print(f"最终资金: {final_value:,.2f}")
    print(f"总收益率: {total_return*100:.2f}%")
    print(f"年化收益: {annual_return*100:.2f}%")
    print(f"最大回撤: {max_drawdown*100:.2f}%")
    print(f"夏普比率: {sharpe:.2f}")
    
    print(f"\n总交易次数: {len(records)}")
    print(f"盈利次数: {len(records[records['return'] > 0])}")
    print(f"亏损次数: {len(records[records['return'] <= 0])}")
    
    if len(records) > 0:
        win_rate = len(records[records['return'] > 0]) / len(records) * 100
        print(f"胜率: {win_rate:.2f}%")
        print(f"\n平均收益率: {records['return'].mean()*100:.2f}%")
        print(f"最大单次盈利: {records['return'].max()*100:.2f}%")
        print(f"最大单次亏损: {records['return'].min()*100:.2f}%")


def main():
    start_time = time.time()
    
    # 1. 加载数据
    close_df, open_df, high_df, circ_mv_df = load_all_stock_data()
    
    # 2. 计算信号
    entries = calculate_signals(close_df, open_df, high_df, circ_mv_df)
    
    # 3. 计算止损止盈
    exits, filtered_entries = calculate_exit_signals(close_df, entries)
    
    # 4. 运行回测
    pf = run_backtest(close_df, open_df, filtered_entries, exits)
    
    if pf is None:
        return
    
    # 5. 打印统计
    print_stats(pf, initial_cash)
    
    # 6. 保存结果
    trades_df = pf.trades.records
    trades_df.to_csv('vbt_trades.csv', index=False)
    print(f"\n交易记录已保存到 vbt_trades.csv")
    
    # 保存每日资产数据
    value_df = pf.value()
    if hasattr(value_df, 'to_frame'):
        value_df = value_df.to_frame(name='value')
    value_df.to_csv('vbt_daily_value.csv')
    print(f"每日资产已保存到 vbt_daily_value.csv")
    
    # 绘制收益曲线
    def plot_results(pf, title="回测结果"):
        """绘制回测结果"""
        try:
            import matplotlib.pyplot as plt
            import matplotlib as mpl
            
            # Windows 系统
            try:
                mpl.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'SimSun', 'Arial Unicode MS']
                mpl.rcParams['axes.unicode_minus'] = False
            except:
                pass
            
            # 获取数据
            portfolio_value = pf.value()
            if hasattr(portfolio_value, 'ndim') and portfolio_value.ndim > 1:
                portfolio_value = portfolio_value.sum(axis=1)
            
            values = np.array(portfolio_value).flatten()
            dates = portfolio_value.index
            final_value = values[-1]
            final_return = (final_value / initial_cash - 1) * 100
            
            fig, axes = plt.subplots(2, 1, figsize=(14, 10))
            
            # 图1：资产曲线
            ax1 = axes[0]
            ax1.plot(dates, values / 1e6, 'b-', linewidth=1.5, label='总资产')
            ax1.axhline(y=initial_cash / 1e6, color='gray', linestyle='--', alpha=0.7, label='初始资金')
            ax1.fill_between(dates, initial_cash / 1e6, values / 1e6,
                             where=values >= initial_cash, color='red', alpha=0.3)
            ax1.fill_between(dates, initial_cash / 1e6, values / 1e6,
                             where=values < initial_cash, color='green', alpha=0.3)
            ax1.set_ylabel('总资产 (百万元)', fontsize=12)
            ax1.set_title(f'{title}\n最终: {final_value/1e6:.2f}M | 收益: {final_return:.2f}%', fontsize=14)
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            
            # 图2：日收益率
            daily_returns = pf.returns()
            if hasattr(daily_returns, 'ndim') and daily_returns.ndim > 1:
                daily_returns = daily_returns.mean(axis=1)
            returns_arr = np.array(daily_returns).flatten()
            
            ax2 = axes[1]
            ax2.plot(daily_returns.index, returns_arr * 100, 'purple', linewidth=0.8)
            ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
            ax2.set_ylabel('日收益率 (%)', fontsize=12)
            ax2.set_xlabel('日期', fontsize=12)
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            plt.savefig('vbt_returns_curve.png', dpi=150, bbox_inches='tight')
            print(f"\n收益曲线已保存到 vbt_returns_curve.png")
            plt.close()
            
        except ImportError:
            print("提示：安装 matplotlib: pip install matplotlib")
        except Exception as e:
            print(f"绘图失败: {e}")
    
    plot_results(pf, "VectorBT 回测 - 动态持仓")
    
    elapsed = time.time() - start_time
    print(f"\n总耗时: {elapsed:.2f} 秒")


if __name__ == '__main__':
    main()