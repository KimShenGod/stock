import os
import copy
import time
import pickle
import talib
import pandas as pd
import numpy as np
import CeLue
import func
import user_config as ucfg
from rqalpha.apis import *
from rqalpha import run_func
from tqdm import tqdm
from rich import print as rprint

# 回测变量定义
start_date = "2013-01-01"  # 回测起始日期
end_date = "2022-12-31"  # 回测结束日期
stock_money = 10000000  # 股票账户初始资金
xiadan_percent = 0.1  # 设定买入总资产百分比的股票份额
xiadan_target_value = 100000  # 设定具体股票买入持有总金额
# 下单模式 买入总资产百分比的股票份额，或买入持有总金额的股票， 'order_percent' or 'order_target_value'
order_type = 'order_target_value'

# 实时选股回测配置
# 选股策略配置：选择要使用的策略组合
# 可选策略：'1'=选股策略, '2'=买入信号策略, '3'=高开涨停策略, '4'=上一交易日涨停策略
#          '5'=今日涨停策略, '6'=小市值策略, '7'=换手率策略, '8'=MACD周线金叉
#          '9'=MACD日线金叉, '10'=持续上升策略, '11'=周线MACD区间
# 例如：selection_strategies = ['1', '2'] 表示先用策略1筛选，再用策略2筛选
selection_strategies = ['1', '2']  # 默认使用选股策略+买入信号策略
max_selected_stocks = 10  # 每次最多买入选出的股票数量
reselect_after_sell = True  # 卖出后是否立即重新选股买入

rq_result_filename = "rq_result/" + time.strftime("%Y-%m-%d_%H%M%S", time.localtime()) + "+" + "start_date" + str(start_date)
rq_result_filename += "+" + order_type + "_" + (str(xiadan_percent) if order_type == 'order_percent' else str(xiadan_target_value))
rq_result_filename += "_实时选股_" + "-".join(selection_strategies)

os.mkdir("rq_result") if not os.path.exists("rq_result") else None
os.remove('temp.csv') if os.path.exists("temp.csv") else None

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

# 策略函数映射
STRATEGY_FUNCTIONS = {
    '1': CeLue.stockSelectionStrategy,
    '2': CeLue.buySignalStrategy,
    '3': CeLue.highOpenLimitUpStrategy,
    '4': CeLue.prevDayLimitUpStrategy,
    '5': CeLue.todayLimitUpStrategy,
    '6': CeLue.smallMarketCapStrategy,
    '7': CeLue.turnoverRateStrategy,
    '8': CeLue.macdWeeklyGoldenCrossStrategy,
    '9': CeLue.macdDailyGoldenCrossStrategy,
    '10': CeLue.continuousRiseWithNearHighStrategy,
    '11': CeLue.macdWeeklyRangeStrategy
}


def update_stockcode(stockcode):
    """转换股票代码格式以匹配rqalpha"""
    if stockcode[0:1] == '6':
        stockcode = stockcode + ".XSHG"
    else:
        stockcode = stockcode + ".XSHE"
    return stockcode


def get_stock_data_from_pickle(stockcode, end_date):
    """从pickle文件读取股票数据，并按回测日期截断"""
    pklfile = ucfg.tdx['pickle'] + os.sep + stockcode + '.pkl'
    if not os.path.exists(pklfile):
        return None
    
    try:
        df_stock = pd.read_pickle(pklfile)
        df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')
        df_stock.set_index('date', drop=False, inplace=True)
        
        # 按回测日期截断数据
        current_dt = pd.to_datetime(end_date)
        df_stock = df_stock[df_stock.index <= current_dt]
        
        if len(df_stock) < 1:
            return None
        return df_stock
    except Exception as e:
        return None


def get_hs300_signal(end_date):
    """获取沪深300信号"""
    try:
        df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', 
                               index_col=None, encoding='gbk', dtype={'code': str})
        df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')
        df_hs300.set_index('date', drop=False, inplace=True)
        
        # 按回测日期截断
        current_dt = pd.to_datetime(end_date)
        df_hs300 = df_hs300[df_hs300.index <= current_dt]
        
        if len(df_hs300) < 1:
            return True  # 默认允许买入
        
        hs300_signal = CeLue.hs300SignalStrategy(df_hs300)
        return hs300_signal.iat[-1] if len(hs300_signal) > 0 else True
    except Exception as e:
        return True  # 默认允许买入


def run_selection_for_single_stock(stockcode, current_date, hs300_signal, df_gbbq, cw_dict, selected_strategies):
    """
    对单个股票执行选股策略
    返回: (是否满足选股条件, 是否满足买入条件, df_stock)
    """
    df_stock = get_stock_data_from_pickle(stockcode, current_date)
    if df_stock is None or len(df_stock) < 10:
        return False, False, None
    
    # 复制数据避免修改原数据
    df_stock = df_stock.copy()
    
    # 检查是否有股本变迁数据，如果有则重新复权
    stock_gbbq = df_gbbq[df_gbbq['code'] == stockcode]
    if not stock_gbbq.empty:
        try:
            df_stock = func.make_fq(stockcode, df_stock, stock_gbbq, cw_dict)
        except:
            pass
    
    # 执行选股策略
    selection_result = True
    buy_signal_result = True
    
    for strategy_id in selected_strategies:
        strategy_func = STRATEGY_FUNCTIONS.get(strategy_id)
        if strategy_func is None:
            continue
        
        try:
            if strategy_id == '1':  # 选股策略
                result = strategy_func(df_stock, start_date='', end_date='', mode='')
            elif strategy_id == '2':  # 买入信号策略
                # 需要先获取HS300信号对应的序列
                result = strategy_func(df_stock, hs300_signal, start_date='', end_date='')
            else:
                result = strategy_func(df_stock, start_date='', end_date='', mode='')
            
            # 检查结果
            if isinstance(result, pd.Series) and len(result) > 0:
                signal = result.iat[-1]
                if not signal:
                    selection_result = False
                if strategy_id == '2':
                    buy_signal_result = signal
            elif isinstance(result, bool):
                if not result:
                    selection_result = False
                if strategy_id == '2':
                    buy_signal_result = result
        except Exception as e:
            selection_result = False
            buy_signal_result = False
    
    return selection_result, buy_signal_result, df_stock


def check_sell_signal_for_position(stockcode, buy_date, current_date, df_gbbq, cw_dict):
    """
    检查持仓股是否满足卖出条件
    返回: (是否满足卖出条件, 卖出原因)
    """
    df_stock = get_stock_data_from_pickle(stockcode, current_date)
    if df_stock is None or len(df_stock) < 10:
        return False, ""
    
    # 复制数据避免修改原数据
    df_stock = df_stock.copy()
    
    # 检查是否有股本变迁数据，如果有则重新复权
    stock_gbbq = df_gbbq[df_gbbq['code'] == stockcode]
    if not stock_gbbq.empty:
        try:
            df_stock = func.make_fq(stockcode, df_stock, stock_gbbq, cw_dict)
        except:
            pass
    
    # 计算买入信号（需要从买入日开始计算）
    try:
        buy_signal = CeLue.buySignalStrategy(df_stock, pd.Series([True]*len(df_stock)), 
                                            start_date='', end_date='')
        sell_signal = CeLue.sellSignalStrategy(df_stock, buy_signal, 
                                              start_date='', end_date='')
        
        if len(sell_signal) > 0 and sell_signal.iat[-1]:
            return True, "策略卖出信号"
    except:
        pass
    
    return False, ""


# 在这个方法中编写任何的初始化逻辑。context对象将会在你的算法策略的任何方法之间做传递。
def init(context):
    # 在context中保存全局变量
    context.percent = xiadan_percent  # 设定买入比例
    context.target_value = xiadan_target_value  # 设定具体股票总买入市值
    context.order_type = order_type  # 下单模式
    context.selection_strategies = selection_strategies  # 选股策略
    context.max_selected_stocks = max_selected_stocks  # 最大持仓数量
    context.reselect_after_sell = reselect_after_sell  # 卖出后重新选股
    
    # 读取股票列表
    stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_lday'])]
    # 剔除科创板股票
    stocklist = [s for s in stocklist if s[:2] != '68']
    context.stocklist = stocklist
    print(f"初始化完成，共 {len(stocklist)} 只候选股票")
    
    # 读取股本变迁数据
    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', 
                          encoding='gbk', dtype={'code': str})
    context.df_gbbq = df_gbbq
    
    # 读取权息字典
    try:
        cw_dict = func.readall_local_cwfile()
    except:
        cw_dict = {}
    context.cw_dict = cw_dict
    
    # 初始化当日选股结果
    context.selected_stocks = []  # 当日选出的股票
    context.checked_stocks = set()  # 已检查过的股票（避免重复选入）
    context.stock_pnl = pd.DataFrame()  # 个股盈亏记录


# before_trading此函数会在每天策略交易开始前被调用，当天只会被调用一次
def before_trading(context):
    context.stock_pnl = pd.DataFrame()
    current_date = context.now.strftime('%Y-%m-%d')
    print(f"\n{'='*60}")
    print(f"日期: {current_date}")
    
    # 获取沪深300信号
    hs300_signal = get_hs300_signal(current_date)
    if hs300_signal:
        print("沪深300信号: 允许买入")
    else:
        print("沪深300信号: 不允许买入")
    
    # 获取当前持仓列表
    current_positions = list(context.portfolio.positions.keys())
    print(f"当前持仓: {len(current_positions)} 只")
    
    # 检查持仓股的卖出条件
    stocks_to_sell = []
    for stock_code in current_positions:
        # 转换代码格式
        raw_code = stock_code.replace(".XSHG", "").replace(".XSHE", "")
        
        # 获取该股的买入日期（从持仓信息中获取）
        position = context.portfolio.positions[stock_code]
        buy_date = None
        # 尝试从订单历史获取买入日期
        try:
            orders = get_orders()
            for order_id, order in orders.items():
                if order.order_book_id == stock_code and order.side == 'BUY':
                    buy_date = order.datetime.strftime('%Y-%m-%d') if hasattr(order.datetime, 'strftime') else str(order.datetime)[:10]
                    break
        except:
            pass
        
        # 检查卖出条件
        should_sell, sell_reason = check_sell_signal_for_position(
            raw_code, buy_date, current_date, context.df_gbbq, context.cw_dict
        )
        if should_sell:
            stocks_to_sell.append((stock_code, sell_reason))
            print(f"  卖出信号: {stock_code} - {sell_reason}")
    
    context.stocks_to_sell = stocks_to_sell
    
    # 执行选股（只有在沪深300信号允许时才选股）
    if hs300_signal:
        print("开始执行选股策略...")
        selected = []
        checked_count = 0
        max_check = 500  # 最多检查500只股票以提高性能
        
        for stockcode in context.stocklist:
            if checked_count >= max_check:
                break
            if stockcode in context.checked_stocks:
                continue
            
            checked_count += 1
            selection_result, buy_signal_result, _ = run_selection_for_single_stock(
                stockcode, current_date, hs300_signal, context.df_gbbq, context.cw_dict, 
                context.selection_strategies
            )
            
            if selection_result and buy_signal_result:
                selected.append(stockcode)
                context.checked_stocks.add(stockcode)
                if len(selected) >= context.max_selected_stocks:
                    break
        
        context.selected_stocks = selected
        print(f"选股完成，选出 {len(selected)} 只股票: {selected}")
    else:
        context.selected_stocks = []
        print("沪深300信号不允许选股")


# 你选择的证券的数据更新将会触发此段逻辑，例如日或分钟历史数据切片或者是实时数据切片更新
def handle_bar(context, bar_dict):
    current_date = context.now.strftime('%Y-%m-%d')
    
    # 1. 卖出持仓中满足卖出条件的股票
    if hasattr(context, 'stocks_to_sell') and context.stocks_to_sell:
        for stock_code, sell_reason in context.stocks_to_sell:
            position = context.portfolio.positions.get(stock_code)
            if position and position.quantity > 0:
                # 卖出全部持仓
                order_result_obj = order_target_value(stock_code, 0)
                if order_result_obj is not None and order_result_obj.unfilled_quantity == 0:
                    print(f"卖出: {stock_code} - {sell_reason}")
                    
                    # 记录卖出盈亏
                    buy_price = position.avg_price
                    sell_price = bar_dict[stock_code].close
                    pnl = (sell_price - buy_price) * position.quantity
                    pnl_rate = (sell_price / buy_price - 1) if buy_price > 0 else 0
                    
                    series = pd.Series(data={
                        "trading_datetime": context.now,
                        "order_book_id": stock_code,
                        "side": "SELL",
                        "盈亏金额": pnl,
                        "盈亏率": round(pnl_rate, 4),
                    })
                    new_row_df = pd.DataFrame([series])
                    context.stock_pnl = pd.concat([context.stock_pnl, new_row_df], ignore_index=True)
                    
                    # 如果设置了卖出后重新选股，从已选列表中移除
                    if context.reselect_after_sell and stock_code in context.selected_stocks:
                        context.selected_stocks.remove(stock_code)
        
        # 清空卖出列表
        context.stocks_to_sell = []
    
    # 2. 买入选出的股票
    if hasattr(context, 'selected_stocks') and context.selected_stocks:
        for stock_code in context.selected_stocks[:]:
            # 转换代码格式
            rq_code = update_stockcode(stock_code)
            
            # 检查是否已持仓
            position = context.portfolio.positions.get(rq_code)
            if position and position.quantity > 0:
                # 已有持仓，跳过
                context.selected_stocks.remove(stock_code)
                continue
            
            # 检查是否停牌
            try:
                if is_suspended(rq_code):
                    print(f"{rq_code} 停牌，跳过")
                    continue
            except:
                pass
            
            # 检查可用资金
            available_cash = context.portfolio.cash
            if available_cash < context.target_value * 0.5:  # 资金不足
                print(f"资金不足，跳过买入")
                break
            
            # 执行买入
            if context.order_type == 'order_percent':
                order_result_obj = order_percent(rq_code, context.percent)
            else:
                order_result_obj = order_target_value(rq_code, context.target_value)
            
            # 处理订单结果
            if order_result_obj is None:
                print(f"资金不足，无法买入 {rq_code}")
            elif order_result_obj.unfilled_quantity > 0:
                print(f"{rq_code} 部分成交")
            else:
                print(f"买入: {rq_code}")
                context.selected_stocks.remove(stock_code)
                
                # 记录买入信息
                buy_price = bar_dict[rq_code].close
                series = pd.Series(data={
                    "trading_datetime": context.now,
                    "order_book_id": rq_code,
                    "side": "BUY",
                    "盈亏金额": 0,
                    "盈亏率": 0,
                })
                new_row_df = pd.DataFrame([series])
                context.stock_pnl = pd.concat([context.stock_pnl, new_row_df], ignore_index=True)


# after_trading函数会在每天交易结束后被调用，当天只会被调用一次
def after_trading(context):
    string = f'净值{context.portfolio.total_value:>.2f} '
    string += f'可用{context.portfolio.cash:>.2f} '
    string += f'市值{context.portfolio.market_value:>.2f} '
    # string += f'收益{context.portfolio.total_returns:>.2%} '
    string += f'持股{len(context.portfolio.positions):>d} '
    print(string)

    if len(context.stock_pnl) > 0:
        if os.path.exists('temp.csv'):
            context.stock_pnl.to_csv('temp.csv', encoding='gbk', mode='a', header=False)  # 附加数据，无标题行
        else:
            context.stock_pnl.to_csv('temp.csv', encoding='gbk', header=True)


__config__ = {
    "base": {
        # 回测起始日期
        "start_date": start_date,
        "end_date": end_date,
        # 数据源所存储的文件路径
        "data_bundle_path": "C:/Users/king/.rqalpha/bundle/",
        "strategy_file": "huice.py",
        # 目前支持 `1d` (日线回测) 和 `1m` (分钟线回测)，如果要进行分钟线，请注意是否拥有对应的数据源，目前开源版本是不提供对应的数据源的。
        "frequency": "1d",
        # 启用的回测引擎，目前支持 current_bar (当前Bar收盘价撮合) 和 next_bar (下一个Bar开盘价撮合)
        "matching_type": "current_bar",
        # 运行类型，`b` 为回测，`p` 为模拟交易, `r` 为实盘交易。
        "run_type": "b",
        # 设置策略可交易品种，目前支持 `stock` (股票账户)、`future` (期货账户)，您也可以自行扩展
        "accounts": {
            # 如果想设置使用某个账户，只需要增加对应的初始资金即可
            "stock": stock_money,
        },
        # 设置初始仓位
        "init_positions": {}
    },
    "extra": {
        # 选择日期的输出等级，有 `verbose` | `info` | `warning` | `error` 等选项，您可以通过设置 `verbose` 来查看最详细的日志，
        "log_level": "info",
    },

    "mod": {
        "sys_analyser": {
            "enabled": True,
            "benchmark": "000300.XSHG",
            # "plot": True,
            'plot_save_file': rq_result_filename + ".png",
            "output_file": rq_result_filename + ".pkl",
            # "report_save_path": "rq_result.csv",
        },
        # 策略运行过程中显示的进度条的控制
        "sys_progress": {
            "enabled": False,
            "show": True,
        },
    },
}

start_time = f'程序开始时间：{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'

# 使用 run_func 函数来运行策略
# 此种模式下，您只需要在当前环境下定义策略函数，并传入指定运行的函数，即可运行策略。
# 如果你的函数命名是按照 API 规范来，则可以直接按照以下方式来运行
run_func(**globals())
end_time = f'程序结束时间：{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())}'

# RQAlpha可以输出一个 pickle 文件，里面为一个 dict 。keys 包括
# summary 回测摘要
# stock_portfolios 股票帐号的市值
# future_portfolios 期货帐号的市值
# total_portfolios 总账号的的市值
# benchmark_portfolios 基准帐号的市值
# stock_positions 股票持仓
# future_positions 期货仓位
# benchmark_positions 基准仓位
# trades 交易详情（交割单）
# plots 调用plot画图时，记录的值
result_dict = pd.read_pickle(rq_result_filename + ".pkl")

# 给rq_result.pkl的交割单添加个股盈亏和收益率统计
df_trades = result_dict['trades']
df_temp = pd.read_csv('temp.csv', index_col=0, encoding='gbk').set_index('trading_datetime', drop=False)  # 个股卖出盈亏金额DF
df_temp.index.name = 'datetime'  # 重置index的name
df_temp = pd.merge(df_trades, df_temp, how='right')  # merge，以df_temp为准。相当于更新df_temp
df_trades = pd.merge(df_trades, df_temp, how='left')  # merge，以df_trades为准。相当于更新df_trades
result_dict['trades'] = df_trades
with open(rq_result_filename+".pkl", 'wb') as fobj:
    pickle.dump(result_dict, fobj)
os.remove('temp.csv') if os.path.exists("temp.csv") else None


rprint(result_dict["summary"])
rprint(start_time)
rprint(end_time)
rprint(
    f"回测起点 {result_dict['summary']['start_date']}"
    f"\n回测终点 {result_dict['summary']['end_date']}"
    f"\n回测收益 {result_dict['summary']['total_returns']:>.2%}\t年化收益 {result_dict['summary']['annualized_returns']:>.2%}"
    f"\t基准收益 {result_dict['summary']['benchmark_total_returns']:>.2%}\t基准年化 {result_dict['summary']['benchmark_annualized_returns']:>.2%}"
    f"\t最大回撤 {result_dict['summary']['max_drawdown']:>.2%}"
    f"\n打开程序文件夹下的rq_result.png查看收益走势图")
