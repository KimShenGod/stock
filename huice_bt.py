"""
本地数据回测脚本 - 基于 Backtrader
完全参照 xuangu.py 的选股逻辑
"""
import os
import sys
import time
import pandas as pd
import numpy as np
import datetime
import backtrader as bt
from tqdm import tqdm

import CeLue
import func
import user_config as ucfg

# ============== 回测配置 ==============
start_date = "2024-01-01"  # 回测起始日期
end_date = "2024-12-31"   # 回测结束日期
initial_cash = 400000   # 初始资金

# 实时选股回测配置
selection_strategies = ['3','6']  # 使用的策略组合: '3'=高开涨停策略
max_selected_stocks = 2      # 每次最多持仓股票数量
position_ratio = 0.5        # 每只股票占持仓比例（约50%，2只满仓约100%）
reselect_after_sell = True    # 卖出后是否立即重新选股

# 数据路径（与 xuangu.py 完全一致）
csvdaypath = ucfg.tdx['pickle']  # pickle 格式数据路径

# 策略枚举定义
STRATEGIES = {
    '1': '选股策略',
    '2': '买入信号策略',
    '3': '高开涨停策略',
    '4': '上一交易日涨停策略',
    '5': '今日涨停策略',
    '6': '小市值策略',
    '7': '换手率策略',
    '8': 'MACD周线金叉策略',
    '9': 'MACD日线金叉策略',
    '10': '持续上升且接近最高价策略',
    '11': '周线MACD区间策略'
}


# ============== 交易费用设置 ==============
# A股实际交易费用：
# 1. 印花税：0.1%（千分之一），只在卖出时收取
# 2. 过户费：0.001%（万分之一），上海股票收取，买卖都收
# 3. 佣金（券商佣金）：0.03%（万分之三），有最低5元限制，买卖都收

STAMP_TAX_RATE = 0.001     # 印花税 0.1%（只在卖出时收取）
TRANSFER_FEE_RATE = 0.00001  # 过户费 0.001%（万分之一，上海股票）
COMMISSION_RATE = 0.0003    # 佣金 0.03%（万分之三）
COMMISSION_MIN = 5          # 佣金最低5元


class AShareCommission(bt.CommInfoBase):
    """
    A股交易费用计算
    - 印花税：只在卖出时收取
    - 过户费：买卖都收（仅上海股票）
    - 佣金：买卖都收，有最低限制
    """
    params = (
        ('stamp_tax', STAMP_TAX_RATE),
        ('transfer_fee', TRANSFER_FEE_RATE),
        ('commission', COMMISSION_RATE),
        ('commission_min', COMMISSION_MIN),
    )
    
    def getcommission(self, size, price, pseudoexec=False):
        """计算佣金（买卖都收）"""
        commission = abs(size) * price * self.params.commission
        return max(commission, self.params.commission_min)
    
    def getcommission_buy(self, size, price, pseudoexec=False):
        """计算买入时的佣金"""
        return self.getcommission(size, price, pseudoexec)
    
    def getcommission_sell(self, size, price, pseudoexec=False):
        """计算卖出时的费用（佣金 + 印花税 + 过户费）"""
        # 佣金
        commission = abs(size) * price * self.params.commission
        commission = max(commission, self.params.commission_min)
        # 印花税（只在卖出时）
        stamp_tax = abs(size) * price * self.params.stamp_tax
        # 过户费
        transfer_fee = abs(size) * price * self.params.transfer_fee
        return commission + stamp_tax + transfer_fee


def run_single_strategy(stockcode, check_date, strategy_id):
    """
    对单只股票在指定日期执行单个策略
    返回该日期的选股结果
    """
    try:
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        if not os.path.exists(pklfile):
            return False
        
        df_stock = pd.read_pickle(pklfile)
        
        # 关键：设置日期索引并按日期升序排列（与 xuangu.py 一致）
        df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')
        df_stock = df_stock.sort_values('date').reset_index(drop=True)  # 升序排列
        df_stock.set_index('date', drop=False, inplace=True)
        
        # 筛选到 check_date 为止的数据
        df_check = df_stock[df_stock['date'] <= check_date].copy()
        if len(df_check) < 2:
            return False
        
        # 重建索引以便策略函数处理
        df_check = df_check.reset_index(drop=True)
        df_check['date'] = pd.to_datetime(df_check['date'])
        df_check.set_index('date', drop=False, inplace=True)
        
        # 根据策略ID执行对应的策略
        if strategy_id == '1':
            result = CeLue.stockSelectionStrategy(df_check, start_date='', end_date='', mode='fast')
        elif strategy_id == '2':
            result = CeLue.buySignalStrategy(df_check, True, start_date='', end_date='')
        elif strategy_id == '3':
            result = CeLue.highOpenLimitUpStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '4':
            result = CeLue.prevDayLimitUpStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '5':
            result = CeLue.todayLimitUpStrategy(df_check, start_date='', end_date='', mode='fast')
        elif strategy_id == '6':
            result = CeLue.smallMarketCapStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '7':
            result = CeLue.turnoverRateStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '8':
            result = CeLue.macdWeeklyGoldenCrossStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '9':
            result = CeLue.macdDailyGoldenCrossStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '10':
            result = CeLue.continuousRiseWithNearHighStrategy(df_check, start_date='', end_date='')
        elif strategy_id == '11':
            result = CeLue.macdWeeklyRangeStrategy(df_check, start_date='', end_date='')
        else:
            return False
        
        # 解析结果
        if isinstance(result, pd.Series) and len(result) > 0:
            return bool(result.iat[-1])
        elif isinstance(result, bool):
            return bool(result)
        else:
            return False
            
    except Exception as e:
        return False


def get_stock_circulation_mv(stockcode, check_date):
    """
    获取股票在指定日期的流通市值
    返回流通市值（单位：亿元），如果无法获取返回inf（大市值排后面）
    """
    try:
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        if not os.path.exists(pklfile):
            return float('inf')
        
        df_stock = pd.read_pickle(pklfile)
        df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')
        
        # 筛选到 check_date 为止的数据
        df_check = df_stock[df_stock['date'] <= check_date].copy()
        if len(df_check) < 1:
            return float('inf')
        
        # 获取最近一天的流通市值
        latest_row = df_check.iloc[-1]
        mv = latest_row.get('流通市值', None)
        
        if mv is None or pd.isna(mv):
            return float('inf')
        
        # 流通市值单位是元，转换为亿元
        return mv / 1e8
        
    except Exception as e:
        return float('inf')


class SelectionStrategy(bt.Strategy):
    """选股策略"""
    
    params = (
        ('selection_strategies', ['3']),
        ('max_selected_stocks', 2),
        ('position_ratio', 0.5),  # 每只股票约50%
        ('printlog', True),
        ('stocklist', []),
    )
    
    def __init__(self):
        self.selected_stocks = []  # 当前选中的股票
        self.current_date = None
        self.buy_bars = {}  # 记录买入时的bar索引
        self.buy_prices = {}  # 记录买入价格
        self.trades_log = []  # 交易日志
        self.total_checked = 0  # 统计检查过的股票数
        self.total_selected = 0  # 统计选中过的股票数
        self.daily_selected = {}  # 每日选股记录
        self.daily_values = []    # 每日资产记录
        self.pending_sell_stocks = {}  # 待卖出股票 {stockcode: (sell_reason, buy_price, buy_size)}
        
    def log(self, txt, dt=None):
        if self.params.printlog:
            dt = dt or self.datas[0].datetime.date(0)
            print(f'{dt.isoformat()} {txt}')
    
    def prenext(self):
        self.next()
    
    def next(self):
        """每个交易日执行"""
        # 获取当前日期
        dt = self.datas[0].datetime.date(0)
        if self.current_date == dt:
            return
        self.current_date = dt
        
        # 0. 执行待卖出订单（次日开盘价卖出）
        self._execute_pending_sells()
        
        # 1. 检查持仓股的卖出条件
        self._check_sell_signals()
        
        # 2. 检查是否需要选股
        self._check_selection()
        
        # 3. 执行买入
        self._execute_buy()
        
        # 4. 记录每日资产
        self._record_daily_value()
        
        # 5. 打印状态（每20天打印一次）
        if len(self) % 20 == 0:
            self._print_status()
    
    def _check_sell_signals(self):
        """检查持仓股是否满足卖出条件"""
        for data in self.datas:
            stockcode = data._name
            position = self.getposition(data)
            
            if position.size > 0:
                buy_price = position.price
                current_price = data.close[0]
                prev_close = data.close[-1] if len(data) > 1 else current_price
                pnl_pct = (current_price - buy_price) / buy_price * 100
                
                # 获取持仓天数
                buy_bar = self.buy_bars.get(stockcode, len(self))
                hold_days = len(self) - buy_bar
                
                # 检查是否跌停（跌停无法卖出）
                is_limit_down = (prev_close > 0 and current_price <= prev_close * 0.9 * 1.005)
                
                should_sell = False
                sell_reason = ""
                
                # 止损止盈（止损-5%，止盈+30%，持满30天）
                if pnl_pct < -5:
                    should_sell = True
                    sell_reason = f"止损 {pnl_pct:.2f}%"
                elif pnl_pct > 30:
                    should_sell = True
                    sell_reason = f"止盈 {pnl_pct:.2f}%"
                elif hold_days > 30:
                    should_sell = True
                    sell_reason = f"持满 {hold_days} 天"
                
                if should_sell:
                    if is_limit_down:
                        # 跌停无法卖出，延后到下一交易日
                        if len(self) % 50 == 0:
                            self.log(f"[跌停] {stockcode} 无法卖出")
                        continue
                    
                    # 记录卖出信号，下一交易日以开盘价卖出
                    self.pending_sell_stocks[stockcode] = {
                        'data': data,
                        'size': position.size,
                        'reason': sell_reason,
                        'pnl_pct': round(pnl_pct, 2)
                    }
                    self.log(f"[卖出信号] {stockcode} {sell_reason} (次日开始卖出)")
    
    def _execute_pending_sells(self):
        """执行待卖出订单（次日开盘价卖出）"""
        for stockcode, sell_info in list(self.pending_sell_stocks.items()):
            data = sell_info['data']
            size = sell_info['size']
            sell_reason = sell_info['reason']
            pnl_pct = sell_info['pnl_pct']
            
            # 检查是否还有持仓
            position = self.getposition(data)
            if position.size != size:
                # 持仓已变化，从待卖出列表移除
                del self.pending_sell_stocks[stockcode]
                continue
            
            # 以开盘价卖出
            sell_price = data.open[0]
            
            # 如果开盘涨停/跌停，无法卖出
            prev_close = data.close[-1] if len(data) > 1 else sell_price
            if sell_price >= prev_close * 1.1 * 0.995 or sell_price <= prev_close * 0.9 * 1.005:
                # 涨停/跌停无法卖出，保持在待卖出列表
                if len(self) % 50 == 0:
                    self.log(f"[涨跌停] {stockcode} 开盘价{sell_price:.2f} 无法卖出")
                continue
            
            self.sell(data=data, size=size)
            
            # 计算实际收益率（基于买入价和卖出价）
            actual_pnl_pct = (sell_price - self.buy_prices.get(stockcode, position.price)) / self.buy_prices.get(stockcode, position.price) * 100
            
            self.log(f"[卖出] {stockcode} @{sell_price:.2f}x{size} ({sell_reason})")
            self.trades_log.append({
                'date': str(self.current_date),
                'stock': stockcode,
                'action': 'SELL',
                'price': sell_price,
                'size': size,
                'pnl_pct': round(actual_pnl_pct, 2),
                'reason': sell_reason
            })
            
            del self.pending_sell_stocks[stockcode]
            if stockcode in self.selected_stocks:
                self.selected_stocks.remove(stockcode)
    
    def _check_selection(self):
        """检查是否需要选股"""
        # 计算可用仓位
        current_positions = len([d for d in self.datas if self.getposition(d).size > 0])
        available_slots = self.params.max_selected_stocks - current_positions
        
        if available_slots <= 0:
            return
        
        # 执行选股（按流通市值排序）
        check_date = pd.Timestamp(self.current_date)
        
        # 第一步：收集所有符合条件的股票及其流通市值
        candidate_stocks = []  # [(stockcode, circulation_mv), ...]
        checked_count = 0
        
        for stockcode in self.params.stocklist:
            # 检查是否已持仓
            has_position = False
            for data in self.datas:
                if data._name == stockcode and self.getposition(data).size > 0:
                    has_position = True
                    break
            if has_position:
                continue
            
            checked_count += 1
            
            # 执行策略检查（"与"关系：所有策略都满足才选中）
            ok = True
            strategy_results = {}
            for strategy_id in self.params.selection_strategies:
                result = run_single_strategy(stockcode, check_date, strategy_id)
                strategy_results[strategy_id] = result
                if not result:
                    ok = False
            
            if ok:
                # 获取流通市值
                circ_mv = get_stock_circulation_mv(stockcode, check_date)
                
                # 策略6（小市值策略）需要流通市值 > 20亿
                if '6' in self.params.selection_strategies:
                    if circ_mv < 20:
                        ok = False
                
                if ok:
                    candidate_stocks.append((stockcode, circ_mv))
                    self.total_selected += 1
                    if len(self) % 50 == 0:  # 减少日志输出
                        strategy_names = [STRATEGIES.get(s, s) for s in self.params.selection_strategies]
                        self.log(f"[选中] {stockcode} 流通市值:{circ_mv:.2f}亿 ({'+'.join(strategy_names)})")
        
        # 第二步：按流通市值排序（小的在前）
        candidate_stocks.sort(key=lambda x: x[1])
        
        # 第三步：取前N个
        selected = [code for code, mv in candidate_stocks[:available_slots]]
        
        self.total_checked += checked_count
        self.selected_stocks.extend(selected)
        
        # 记录每日选股数量
        self.daily_selected[str(self.current_date)] = len(selected)
        
        if len(self) % 50 == 0:  # 减少日志输出
            self.log(f"[选股完成] 空余仓位:{available_slots} 检查:{checked_count} 选出:{len(selected)}")
    
    def _execute_buy(self):
        """执行买入"""
        if not self.selected_stocks:
            return
        
        # 计算每只股票的目标市值（按总资产的比例）
        total_value = self.broker.getvalue()
        target_value = total_value * self.params.position_ratio
        
        available_cash = self.broker.getcash()
        
        for stockcode in self.selected_stocks[:]:
            # 检查资金
            if available_cash < target_value * 0.3:
                if len(self) % 50 == 0:
                    self.log(f"[资金不足] 可用:{available_cash:,.0f}")
                break
            
            # 找到对应数据
            target_data = None
            for data in self.datas:
                if data._name == stockcode:
                    target_data = data
                    break
            
            if target_data is not None:
                # 优先以开盘价买入（非涨停）
                open_price = target_data.open[0]
                close_price = target_data.close[0]
                prev_close = target_data.close[-1] if len(target_data) > 1 else close_price
                
                # 检查是否涨停（涨停无法买入）
                is_limit_up = (prev_close > 0 and close_price >= prev_close * 1.1 * 0.995)
                
                # 使用开盘价（非涨停时），涨停时使用收盘价
                price = open_price if not is_limit_up else close_price
                
                if is_limit_up:
                    if len(self) % 50 == 0:
                        self.log(f"[涨停] {stockcode} @{price:.2f} 无法买入")
                    self.selected_stocks.remove(stockcode)
                    continue
                
                size = int(target_value / price / 100) * 100
                
                if size > 0 and available_cash >= size * price * 1.002:
                    self.buy(data=target_data, size=size)
                    self.buy_bars[stockcode] = len(self)
                    self.buy_prices[stockcode] = price  # 记录买入价格
                    available_cash -= size * price * 1.002
                    
                    self.log(f"[买入] {stockcode} @{price:.2f}x{size} 目标:{target_value:,.0f}")
                    
                    self.trades_log.append({
                        'date': str(self.current_date),
                        'stock': stockcode,
                        'action': 'BUY',
                        'price': price,
                        'size': size,
                        'pnl_pct': 0,
                        'reason': f'选股买入 目标{self.params.position_ratio*100:.0f}%'
                    })
                    
                    self.selected_stocks.remove(stockcode)
    
    def _print_status(self):
        """打印账户状态"""
        total_value = self.broker.getvalue()
        cash = self.broker.getcash()
        positions = len([d for d in self.datas if self.getposition(d).size > 0])
        
        returns = (total_value / initial_cash - 1) * 100
        self.log(f"[状态] 总资产:{total_value:,.0f} 收益:{returns:.2f}% 现金:{cash:,.0f} 持仓:{positions}只")
    
    def _record_daily_value(self):
        """记录每日资产"""
        total_value = self.broker.getvalue()
        returns = (total_value / initial_cash - 1) * 100
        self.daily_values.append({
            'date': self.current_date,
            'total_value': total_value,
            'returns': returns
        })
    
    def stop(self):
        """回测结束"""
        total_value = self.broker.getvalue()
        returns = (total_value / initial_cash - 1) * 100
        
        print(f"\n{'='*60}")
        print(f"回测结束!")
        print(f"最终资产: {total_value:,.2f}")
        print(f"总收益: {total_value - initial_cash:,.2f}")
        print(f"收益率: {returns:.2f}%")
        print(f"检查股票总数: {self.total_checked}")
        print(f"选中股票总数: {self.total_selected}")
        print(f"交易次数: {len(self.trades_log)}")
        print(f"每日选股统计: {self.daily_selected}")
        
        # 保存交易记录
        if self.trades_log:
            df_trades = pd.DataFrame(self.trades_log)
            df_trades.to_csv('trades_log.csv', index=False, encoding='utf-8-sig')
            print(f"交易记录已保存到 trades_log.csv")
        
        # 绘制收益曲线
        self._plot_returns()
    
    def _plot_returns(self):
        """绘制收益曲线"""
        try:
            import matplotlib.pyplot as plt
            import matplotlib.dates as mdates
            
            # 设置中文字体
            plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
            plt.rcParams['axes.unicode_minus'] = False
            
            # 准备数据
            dates = [item['date'] for item in self.daily_values]
            values = [item['total_value'] for item in self.daily_values]
            returns = [item['returns'] for item in self.daily_values]
            
            # 创建图表
            fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), sharex=True)
            fig.suptitle('Backtest Returns Curve', fontsize=14, fontweight='bold')
            
            # 图1：总资产曲线
            ax1.plot(dates, values, 'b-', linewidth=1.5, label='Total Value')
            ax1.axhline(y=initial_cash, color='gray', linestyle='--', alpha=0.7, label='Initial Cash')
            ax1.fill_between(dates, initial_cash, values, where=[v >= initial_cash for v in values], 
                           color='green', alpha=0.3, label='Profit')
            ax1.fill_between(dates, initial_cash, values, where=[v < initial_cash for v in values], 
                           color='red', alpha=0.3, label='Loss')
            ax1.set_ylabel('Total Value (CNY)')
            ax1.set_title(f'Final: {values[-1]:,.0f} | Return: {returns[-1]:.2f}%')
            ax1.legend(loc='upper left')
            ax1.grid(True, alpha=0.3)
            ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/1e6:.1f}M'))
            
            # 图2：收益率曲线
            ax2.plot(dates, returns, 'purple', linewidth=1.5, label='Returns %')
            ax2.axhline(y=0, color='gray', linestyle='--', alpha=0.7)
            ax2.fill_between(dates, 0, returns, where=[r >= 0 for r in returns], 
                            color='green', alpha=0.3, label='Profit')
            ax2.fill_between(dates, 0, returns, where=[r < 0 for r in returns], 
                            color='red', alpha=0.3, label='Loss')
            ax2.set_ylabel('Returns (%)')
            ax2.set_xlabel('Date')
            ax2.legend(loc='upper left')
            ax2.grid(True, alpha=0.3)
            
            # 格式化x轴日期
            ax2.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
            ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=1))
            plt.xticks(rotation=45)
            
            plt.tight_layout()
            
            # 保存图片
            plt.savefig('returns_curve.png', dpi=150, bbox_inches='tight')
            print(f"收益曲线已保存到 returns_curve.png")
            
            # 保存数据
            df_returns = pd.DataFrame(self.daily_values)
            df_returns.to_csv('daily_returns.csv', index=False, encoding='utf-8-sig')
            print(f"每日收益数据已保存到 daily_returns.csv")
            
            plt.close()
            
        except ImportError:
            print("提示：安装 matplotlib 可绘制收益曲线: pip install matplotlib")
        except Exception as e:
            print(f"绘图失败: {e}")


class LocalData(bt.feeds.PandasData):
    """本地 Pandas 数据格式"""
    params = (
        ('datetime', 'date'),
        ('open', 'open'),
        ('high', 'high'),
        ('low', 'low'),
        ('close', 'close'),
        ('volume', 'vol'),
        ('openinterest', -1),
    )


def run_backtest():
    """运行回测"""
    print("="*60)
    print("本地数据回测系统 - 基于 Backtrader")
    print("="*60)
    print(f"回测期间: {start_date} ~ {end_date}")
    print(f"初始资金: {initial_cash:,.2f}")
    print(f"选股策略: {[STRATEGIES.get(s, s) for s in selection_strategies]}")
    print()
    
    # 获取股票列表
    stocklist = [i[:-4] for i in os.listdir(csvdaypath) if i.endswith('.pkl')]
    stocklist = [s for s in stocklist if s[:2] != '68']  # 剔除科创板
    
    print(f"发现 {len(stocklist)} 只股票")
    
    # 创建 Cerebro
    cerebro = bt.Cerebro()
    cerebro.broker.setcash(initial_cash)
    
    # 设置A股实际交易费用
    cerebro.broker.setcommission(commission=0)  # 禁用默认佣金，使用自定义
    cerebro.broker.addcommissioninfo(AShareCommission())
    
    # 设置滑点（可选）
    cerebro.broker.set_slippage_perc(0.001)  # 0.1%滑点
    
    # 添加策略
    cerebro.addstrategy(
        SelectionStrategy,
        selection_strategies=selection_strategies,
        max_selected_stocks=max_selected_stocks,
        position_ratio=position_ratio,
        printlog=True,
        stocklist=stocklist,
    )
    
    # 添加数据
    print("加载数据...")
    added_count = 0
    for stockcode in tqdm(stocklist, desc="加载数据"):
        df = _load_stock_data(stockcode)
        if df is not None and len(df) >= 10:
            data = LocalData(dataname=df, name=stockcode)
            cerebro.adddata(data)
            added_count += 1
    
    print(f"成功加载 {added_count} 只股票")
    
    if added_count == 0:
        print("错误: 没有可用的股票数据")
        return
    
    # 运行回测
    print("\n开始回测...")
    sys.stdout.flush()
    
    start_time = time.time()
    results = cerebro.run()
    elapsed_time = time.time() - start_time
    
    print(f"\n回测完成!")
    print(f"总耗时: {elapsed_time:.2f} 秒 ({elapsed_time/60:.1f} 分钟)")
    print("\n程序退出.")


def _load_stock_data(stockcode):
    """加载单只股票数据"""
    pklfile = os.path.join(csvdaypath, f"{stockcode}.pkl")
    if not os.path.exists(pklfile):
        return None
    
    try:
        df = pd.read_pickle(pklfile)
        df['date'] = pd.to_datetime(df['date'])
        
        # 按日期过滤
        mask = (df['date'] >= start_date) & (df['date'] <= end_date)
        df = df[mask].copy()
        
        if len(df) < 1:
            return None
        
        # 按日期升序排序
        df = df.sort_values('date').reset_index(drop=True)
        
        return df
    except:
        return None


if __name__ == '__main__':
    run_backtest()
