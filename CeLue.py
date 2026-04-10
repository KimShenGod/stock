"""
此为策略模板文件。你自己写策略后，一定要保存为celue.py
celue.py文件不直接执行，通过xuangu.py或celue_save.py调用

个人实际策略不分享。

MA函数返回的是值。
其余函数输入、输出都是序列。只有序列才能表现出来和通达信一样的判断逻辑。
HHV/LLV/COUNT使用了rolling函数，性能极差，慎用。

"""
import numpy as np
import talib
import time
import func
from func_TDX import rolling_window, REF, MA, SMA, HHV, LLV, COUNT, EXIST, CROSS, BARSLAST
from rich import print

import pandas as pd
import os
import user_config as ucfg

# 判断今天是否是中国股市的交易日
def is_trading_day():
    """
    判断今天是否是中国股市的交易日
    使用pandas的bdate_range函数，自动排除周末
    """
    today = pd.Timestamp.today().normalize()
    try:
        # 使用freq='B'（Business Day）直接判断，自动排除周末
        trading_days = pd.bdate_range(start=today, end=today, freq='B')
        return len(trading_days) > 0
    except Exception as e:
        # 如果pandas的bdate_range出错，回退到基本的工作日判断
        return 0 <= today.weekday() <= 4


# 策略列表，包含所有交易策略和选股策略
strategy_list = []



def hs300SignalStrategy(df_hs300, start_date='', end_date=''):
    """
    HS300信号策略：当信号为True时，当日适合买入股票；为False时，当日不适合买入。
    :param df_hs300: DataFrame格式，沪深300指数数据
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :return: 布尔序列，每个元素对应一个交易日的买入信号状态
    """
    if start_date == '':
        start_date = df_hs300.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df_hs300.index[-1]  # 设置为df最后一个日期
    df_hs300 = df_hs300.loc[start_date:end_date]
    hs300_close = df_hs300['close']
    hs300_daily_change = (hs300_close / REF(hs300_close, 1) - 1) * 100
    hs300_signal = ~(hs300_daily_change < -1.5) & ~(hs300_daily_change > 1.5)
    return hs300_signal



def stockSelectionStrategy(df, start_date='', end_date='', mode=None):
    """
    选股策略：从股票池中筛选出符合特定条件的股票
    :param df: DataFrame格式，具体一个股票的数据表，时间列为索引
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，只处理当日数据，用于开盘快速筛选股票
    :return: 布尔序列或布尔值，筛选结果
    """
    try:
        if start_date == '':
            start_date = df.index[0]  # 设置为df第一个日期
        if end_date == '':
            end_date = df.index[-1]  # 设置为df最后一个日期
        df = df.loc[start_date:end_date]

        open_price = df['open']
        high_price = df['high']
        low_price = df['low']
        close_price = df['close']
        if {'换手率'}.issubset(df.columns):  # 无换手率列的股票，只可能是近几个月的新股
            turnover_rate = df['换手率']
        else:
            turnover_rate = 0

        if mode == 'fast':
            # 天数不足500天，收盘价小于9直接返回FALSE
            if close_price.shape[0] < 500 or close_price.iat[-1] < 9:
                return False

            amount_30d_avg = MA(df['amount'] / 10000, 30)  # 30日金额均值
            circulation_mv_billion = df['流通市值'] / 100000000  # 流通市值（亿元）

            ma5 = MA(close_price, 5)  # 5日均线

            # 排除当日涨停的股票
            if df['code'].iloc[0][0:2] == "68" or df['code'].iloc[0][0:2] == "30":
                limit_up_rate = 1.2  # 科创板和创业板涨停幅度20%
            else:
                limit_up_rate = 1.1  # 普通股票涨停幅度10%
            # 检查是否涨停（使用更精确的涨停价计算）
            # 涨停价 = floor(昨收价 * 涨停幅度 * 100 + 0.5) / 100
            prev_close = REF(close_price, 1)
            limit_up_price = np.floor(prev_close * limit_up_rate * 100 + 0.5) / 100
            # 涨停条件：收盘价 >= 涨停价（考虑浮点数精度误差0.01）
            is_limit_up = close_price >= (limit_up_price - 0.01)
            is_limit_up_today = is_limit_up.iat[-1]

            result = is_limit_up_today
        else:
            amount_30d_avg = SMA(df['amount'] / 10000, 30)  # 30日金额均值
            circulation_mv_billion = df['流通市值'] / 100000000  # 流通市值（亿元）
            ma5 = SMA(close_price, 5)  # 5日均线

            # 条件1：股票上市时间足够长且价格适中
            condition1 = (BARSLAST(close_price == 0) > 500) & (df['close'] > 9)

            # 条件2：排除当日涨停的股票
            if df['code'].iloc[0][0:2] == "68" or df['code'].iloc[0][0:2] == "30":
                limit_up_rate = 1.2  # 科创板和创业板涨停幅度20%
            else:
                limit_up_rate = 1.1  # 普通股票涨停幅度10%
            # 检查是否涨停（使用更精确的涨停价计算）
            prev_close = REF(close_price, 1)
            limit_up_price = np.floor(prev_close * limit_up_rate * 100 + 0.5) / 100
            is_limit_up = close_price >= (limit_up_price - 0.01)

            result = condition1 & is_limit_up
        return result
    except Exception as e:
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)



def buySignalStrategy(df, hs300_signal, start_date='', end_date=''):
    """
    买入信号策略：结合沪深300指数信号和多种技术指标，判断个股是否适合买入
    :param df: DataFrame格式，具体一个股票的数据表，时间列为索引
    :param hs300_signal: 布尔序列，沪深300指数信号
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :return: 布尔序列，每个元素对应一个交易日的买入信号状态
    """
    try:
        if start_date == '':
            start_date = df.index[0]  # 设置为df第一个日期
        if end_date == '':
            end_date = df.index[-1]  # 设置为df最后一个日期
        df = df.loc[start_date:end_date]

        if df.shape[0] < 251:  # 小于250日 直接返回false序列
            return pd.Series(index=df.index, dtype=bool)

        # 根据df的索引重建HS300信号，为了与股票交易日期一致
        hs300_signal = pd.Series(hs300_signal, index=df.index, dtype=bool).dropna()

        open_price = df['open']
        high_price = df['high']
        low_price = df['low']
        close_price = df['close']
        turnover_rate = df['换手率']

        # 计算多种均线
        ma5 = SMA(close_price, 5)
        ma10 = SMA(close_price, 10)
        ma20 = SMA(close_price, 20)
        ma60 = SMA(close_price, 60)
        ma120 = SMA(close_price, 120)
        ma250 = SMA(close_price, 250)

        circulation_mv_billion = df['流通市值'] / 100000000  # 流通市值（亿元）

        # 条件1：均线系统条件
        condition1 = (ma120 > -5) & (ma10 < 60) & (ma60 < 10) & (-7 < ma250) & (ma250 < 10)

        # 条件2：价格在MA60上方且有上涨趋势
        condition2 = (close_price > SMA(close_price, 60)) & (close_price < SMA(close_price, 60) * 1.1) & (close_price > open_price)

        # 条件6：涨幅控制条件
        # {20日/200日涨幅小于50%，且收盘价到上穿MA60日的涨幅 除以上穿MA60日到30日收盘最低价 的比 小于1.5倍 }
        llv_200 = LLV(close_price, 200)
        llv_20 = LLV(close_price, 20)
        ma60_cross_day = BARSLAST((REF(close_price, 5) < ma60) & CROSS(close_price, ma60))
        
        # 计算上穿MA60时的MA60值
        ma60_cross_value = pd.Series(index=ma60_cross_day.index, dtype=float)  # 新建序列，传递索引

        i = 0
        for k, v in ma60_cross_day.items():
            try:
                if i - v >= 0 and i < len(ma60_cross_value) and (i - v) < len(ma60):
                    ma60_cross_value.iat[i] = ma60.iat[i - v]
            except IndexError as e:
                # 忽略索引错误，继续执行
                pass
            i = i + 1

        df = pd.concat([df, ma60_cross_day.rename('ma60_cross_day')], axis=1)
        df.insert(df.shape[1], 'ma60_cross_llv', np.NaN)
        for index_date in df.loc[df['ma60_cross_day'] == 0].index.to_list():
            index_int = df.index.get_loc(index_date)
            try:
                # 确保索引不小于0
                start_index = max(0, index_int - 20)
                if start_index < index_int:  # 确保有数据可以计算
                    df.at[index_date, 'ma60_cross_llv'] = df.iloc[start_index:index_int]['close'].min()
            except Exception as e:
                # 忽略计算错误，继续执行
                pass
        df = df.fillna(method='ffill')  # 向下填充无效值
        ma60_cross_llv = df['ma60_cross_llv']

        condition6_3 = ma60_cross_value / ma60_cross_llv
        condition6_4 = close_price / ma60_cross_value
        condition6 = (llv_20 / llv_200 - 1 < 0.5) & (1 < condition6_3 / condition6_4) & (condition6_3 / condition6_4 < 1.5)

        # 条件11：综合选股条件和HS300信号
        stock_selection_result = stockSelectionStrategy(df, start_date, end_date)
        condition11_1 = hs300_signal & stock_selection_result & condition1 & condition2 & condition6
        condition11_2 = COUNT(condition11_1, 10)
        condition11 = condition11_1 & (REF(condition11_2, 1) == 0)

        # 最终买入信号
        buy_signal = condition11

        return buy_signal
    except Exception as e:
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)



def sellSignalStrategy(df, buy_signal, start_date='', end_date=''):
    """
    卖出信号策略：根据不同的风险情况和持有时间，判断个股是否应该卖出
    :param df: DataFrame格式，具体一个股票的数据表，时间列为索引
    :param buy_signal: 布尔序列，买入信号
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :return: 布尔序列，每个元素对应一个交易日的卖出信号状态
    """

    if True not in buy_signal.to_list():  # 买入信号中没有买入点
        return pd.Series(index=buy_signal.index, dtype=bool)

    if start_date == '':
        start_date = df.index[0]  # 设置为df第一个日期
    if end_date == '':
        end_date = df.index[-1]  # 设置为df最后一个日期
    df = df.loc[start_date:end_date]

    open_price = df['open']
    high_price = df['high']
    low_price = df['low']
    close_price = df['close']
    circulation_mv_billion = df['流通市值'] / 100000000  # 流通市值（亿元）

    # 计算多种均线
    ma10 = SMA(close_price, 10)
    ma60 = SMA(close_price, 60)

    # 计算买入后的各种指标
    days_since_buy = BARSLAST(buy_signal)
    buy_price_close = pd.Series(index=close_price.index, dtype=float)
    buy_price_open = pd.Series(index=close_price.index, dtype=float)
    buy_pct = pd.Series(index=close_price.index, dtype=float)
    buy_pct_max = pd.Series(index=close_price.index, dtype=float)
    
    # 填充买入价格
    for i in days_since_buy[days_since_buy == 0].index.to_list()[::-1]:
        buy_price_close.loc[i] = close_price.loc[i]
        buy_price_open.loc[i] = open_price.loc[i]
        buy_price_close.fillna(method='ffill', inplace=True)  # 向下填充无效值
        buy_price_open.fillna(method='ffill', inplace=True)  # 向下填充无效值
        buy_pct = close_price / buy_price_close - 1
        
        # 计算买入后最大涨幅
        for k, v in buy_pct[i:].items():
            if np.isnan(buy_pct_max[k]):
                buy_pct_max[k] = buy_pct[i:k].max()

    # 卖出条件1：跌破MA60且跌破买入日开盘价
    sell_condition1 = (close_price < ma60) & (close_price < buy_price_open)

    # 卖出条件2：最高点小于前低点，表示有向下跳空缺口
    sell_condition2 = (buy_pct < 0.1) & (high_price < REF(low_price, 1))

    # 卖出条件3：持有N天后涨幅小（收益率在1%-3%之间）
    hold_days = circulation_mv_billion.apply(lambda x: 7 if x < 100 else 14)  # 根据市值确定持有天数
    sell_condition3 = (days_since_buy > hold_days) & (buy_pct > 0.01) & (buy_pct < 0.03)

    # 最终卖出信号
    sell_signal = sell_condition1 | sell_condition2 | sell_condition3
    final_sell_signal = pd.Series(index=close_price.index, dtype=bool)
    
    # 循环，第一次出现卖出信号时标记
    for i in days_since_buy[days_since_buy == 0].index.to_list()[::-1]:
        for k, v in sell_signal[i:].items():
            # 排除买入信号当日同时产生卖出信号的极端情况
            if k != i and sell_signal[k]:
                final_sell_signal[k] = True
                break

    return final_sell_signal



def highOpenLimitUpStrategy(df, start_date='', end_date='', mode=None):
    """
    高开涨停策略：筛选出上一个交易日涨停，并且今天高开2个点以上的股票
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index >= start_dt]
            else:
                df_copy = df_copy[df_copy['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index <= end_dt]
            else:
                df_copy = df_copy[df_copy['date'] <= end_dt]

        # 确保数据量足够（至少需要2天历史数据：上一交易日、今天）
        if len(df_copy) < 2:
            return pd.Series([False] * len(df), index=df.index)

        # 获取最新的实时行情数据（如果是盘中选股）
        realtime_today = None
        stock_code = df_copy['code'].iloc[0] if 'code' in df_copy.columns else df['code'].iloc[0]
        
        # 盘中选股模式，尝试获取实时行情数据
        if mode == 'fast':
            # 获取实时行情数据
            try:
                df_today = func.get_tdx_lastestquote([stock_code])
                if not df_today.empty:
                    realtime_today = df_today.iloc[0]
                    print(f'获取到{stock_code}的实时行情数据')
            except Exception as e:
                print(f'获取实时行情数据失败: {e}')
        
        # 确保数据量足够（至少需要2天历史数据：上一交易日、今天）
        if len(df_copy) < 2:
            return pd.Series([False] * len(df), index=df.index)
        
        # 日期关系：
        # prev_day: 上一个交易日（倒数第二天）
        # prev_prev_day: 前前一天（倒数第三天，用于计算上一交易日的涨停价格）
        prev_day = df_copy.iloc[-2] if len(df_copy) >= 2 else df_copy.iloc[-1]
        prev_prev_day = df_copy.iloc[-3] if len(df_copy) >= 3 else prev_day
        
        # 上一交易日的收盘价和最高价
        prev_close = prev_day['close']
        prev_high = prev_day['high']
        
        # 前前一天的收盘价（用于计算上一交易日的涨停价格）
        prev_prev_close = prev_prev_day['close']
        
        # 确定涨停幅度
        # 对于ST股，需要特殊处理涨停幅度为5%
        # 注意：股票代码中不包含ST标识，这里根据涨幅情况动态判断
        limit_up_pct = 0.1  # 默认10%
        
        # 根据股票代码判断市场类型
        if stock_code.startswith('68') or stock_code.startswith('30'):
            # 科创板和创业板，涨停幅度20%
            limit_up_pct = 0.2
        else:
            # 计算上一交易日的实际涨幅
            prev_day_change = (prev_close / prev_prev_close - 1) * 100
            # 如果涨幅接近5%，则可能是ST股
            if abs(prev_day_change - 5.0) < 0.5:
                limit_up_pct = 0.05
            # 如果涨幅接近10%，则是普通股票
            elif abs(prev_day_change - 10.0) < 0.5:
                limit_up_pct = 0.1
        
        # 计算上一交易日的涨停价格（基于前前一天的收盘价）
        limit_up_price = prev_prev_close * (1 + limit_up_pct)
        
        # 判断上一个交易日是否涨停（使用最高价判断更准确）
        # 考虑到价格精度问题，使用近似判断，增加容错空间
        is_prev_limit_up = (prev_high >= limit_up_price * 0.995)
        
        # 判断今天是否高开2个点以上
        is_today_gap_up = False
        
        # 优先使用实时行情数据
        if realtime_today is not None:
            # 使用实时行情数据判断
            today_open = realtime_today['open']
            is_today_gap_up = (today_open >= prev_close * 1.02)
        elif len(df_copy) >= 2:
            # 使用本地数据判断：今天是最后一天，上一交易日是倒数第二天
            # 调整条件为>=2，因为只需要今天和昨天的数据
            today_data = df_copy.iloc[-1]
            today_open = today_data['open']
            is_today_gap_up = (today_open >= prev_close * 1.02)
        
        # 合并条件：上一个交易日涨停且今天高开2个点以上
        final_result = is_prev_limit_up and is_today_gap_up
        
        # 创建结果序列
        result = pd.Series([False] * len(df), index=df.index)
        # 只在最后一天设置结果
        result.iloc[-1] = final_result
        
        return result
    except Exception as e:
        print(f"高开涨停策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)



def prevDayLimitUpStrategy(df, start_date='', end_date='', mode=None):
    """
    上一交易日涨停策略：筛选出上一个交易日涨停的股票，包含ST股（普通ST和创业板ST股）和创业板
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index >= start_dt]
            else:
                df_copy = df_copy[df_copy['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index <= end_dt]
            else:
                df_copy = df_copy[df_copy['date'] <= end_dt]

        # 确保数据量足够（至少需要2天历史数据：上一交易日、今天）
        if len(df_copy) < 2:
            return pd.Series([False] * len(df), index=df.index)

        # 获取股票代码
        stock_code = df_copy['code'].iloc[0] if 'code' in df_copy.columns else df['code'].iloc[0]
        stock_name = df_copy['name'].iloc[0] if 'name' in df_copy.columns else ''
        
        # 日期关系：
        # prev_day: 上一个交易日（倒数第二天）
        # prev_prev_day: 前前一天（倒数第三天，用于计算上一交易日的涨停价格）
        prev_day = df_copy.iloc[-2] if len(df_copy) >= 2 else df_copy.iloc[-1]
        prev_prev_day = df_copy.iloc[-3] if len(df_copy) >= 3 else prev_day
        
        # 上一交易日的收盘价和最高价
        prev_close = prev_day['close']
        prev_high = prev_day['high']
        
        # 前前一天的收盘价（用于计算上一交易日的涨停价格）
        prev_prev_close = prev_prev_day['close']
        
        # 确定涨停幅度
        limit_up_pct = 0.1  # 默认10%
        
        # 根据股票代码和名称判断涨停幅度
        # 科创板和创业板，涨停幅度20%
        if stock_code.startswith('68') or stock_code.startswith('30'):
            limit_up_pct = 0.2
        # ST股，涨停幅度5%
        elif 'ST' in stock_name or '*ST' in stock_name:
            limit_up_pct = 0.05
        else:
            # 计算上一交易日的实际涨幅，用于动态判断是否为ST股
            prev_day_change = (prev_close / prev_prev_close - 1) * 100
            # 如果涨幅接近5%，则可能是ST股
            if abs(prev_day_change - 5.0) < 0.5:
                limit_up_pct = 0.05
        
        # 计算上一交易日的涨停价格（基于前前一天的收盘价）
        limit_up_price = prev_prev_close * (1 + limit_up_pct)
        
        # 判断上一个交易日是否涨停（使用最高价判断更准确）
        # 考虑到价格精度问题，使用近似判断，增加容错空间
        is_prev_limit_up = (prev_high >= limit_up_price * 0.995)
        
        # 创建结果序列
        result = pd.Series([False] * len(df), index=df.index)
        # 只在最后一天设置结果
        result.iloc[-1] = is_prev_limit_up
        
        return result
    except Exception as e:
        print(f"上一交易日涨停策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


def todayLimitUpStrategy(df, start_date='', end_date='', mode=None):
    """
    今日涨停策略：筛选出今日涨停的股票，包含ST股和创业板
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index >= start_dt]
            else:
                df_copy = df_copy[df_copy['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index <= end_dt]
            else:
                df_copy = df_copy[df_copy['date'] <= end_dt]

        # 确保数据量足够
        if len(df_copy) < 2:
            return pd.Series([False] * len(df), index=df.index)

        # 获取股票代码和名称
        stock_code = df_copy['code'].iloc[0] if 'code' in df_copy.columns else df['code'].iloc[0]
        stock_name = df_copy['name'].iloc[0] if 'name' in df_copy.columns else ''
        
        # 日期关系：
        # today: 今日（最后一天）
        # prev_day: 上一个交易日（用于计算今日的涨停价格）
        prev_day = df_copy.iloc[-2]  # 必须使用上一个交易日的数据
        today = df_copy.iloc[-1]
        
        # 上一交易日的收盘价（用于计算今日的涨停价格）
        prev_close = prev_day['close']
        
        # 确定涨停幅度
        limit_up_pct = 0.1  # 默认10%
        
        # 根据股票代码和名称判断涨停幅度
        # 科创板和创业板，涨停幅度20%
        if stock_code.startswith('68') or stock_code.startswith('30'):
            limit_up_pct = 0.2
        # ST股，涨停幅度5%
        elif 'ST' in stock_name or '*ST' in stock_name:
            limit_up_pct = 0.05
        
        # 计算今日的涨停价格（基于上一交易日的收盘价）
        limit_up_price = prev_close * (1 + limit_up_pct)
        
        # 打印调试信息
        # print(f"股票: {stock_code} {stock_name}, 昨收: {prev_close:.2f}, 涨停幅度: {limit_up_pct*100}%, 涨停价: {limit_up_price:.2f}")
        
        # 使用df中已经包含的实时行情数据判断是否涨停
        # 不再单独调用get_tdx_lastestquote获取数据，避免重复请求服务器
        today_price = today['close']
        
        # 如果是盘中选股，df中应该已经包含了最新的实时行情
        # 检查是否有price列（实时行情数据）
        if 'price' in df_copy.columns:
            today_price = today['price']
        
        # 涨停价取两位小数（与实际行情一致）
        limit_up_price_rounded = round(limit_up_price, 2)
        
        # 判断当前价格是否等于涨停价（考虑到价格精度问题，允许0.01元误差）
        is_today_limit_up = (abs(today_price - limit_up_price_rounded) <= 0.01)
        
        # 打印调试信息
        # print(f"价格: {today_price:.2f}, 涨停价: {limit_up_price_rounded:.2f}, 是否涨停: {is_today_limit_up}")
        
        # 创建结果序列
        result = pd.Series([False] * len(df), index=df.index)
        # 只在最后一天设置结果
        result.iloc[-1] = is_today_limit_up
        
        return result
    except Exception as e:
        print(f"今日涨停策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


def smallMarketCapStrategy(df, start_date='', end_date='', mode=None):
    """
    小市值策略：筛选流通市值小于100亿的股票
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index >= start_dt]
            else:
                df_copy = df_copy[df_copy['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index <= end_dt]
            else:
                df_copy = df_copy[df_copy['date'] <= end_dt]

        # 确保数据量足够
        if len(df_copy) < 1:
            return pd.Series([False] * len(df), index=df.index)

        # 日期关系：
        # today: 今日（最后一天）
        today = df_copy.iloc[-1] if len(df_copy) >= 1 else df_copy.iloc[-1]
        
        # 检查流通市值是否存在
        if '流通市值' not in df_copy.columns:
            return pd.Series([False] * len(df), index=df.index)
        
        # 获取今日流通市值
        circulation_mv = today['流通市值']
        
        # 检查流通市值是否小于100亿
        # 流通市值单位通常为元，所以需要转换为亿元
        is_small_cap = (circulation_mv < 10000000000)  # 100亿 = 10000000000元
        
        # 创建结果序列
        result = pd.Series([False] * len(df), index=df.index)
        # 只在最后一天设置结果
        result.iloc[-1] = is_small_cap
        
        return result
    except Exception as e:
        print(f"小市值策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


def turnoverRateStrategy(df, start_date='', end_date='', mode=None):
    """
    换手率策略：筛选最近一个交易日（如果今天是交易日就是今日）换手率大于5%且小于30%的股票
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index >= start_dt]
            else:
                df_copy = df_copy[df_copy['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index <= end_dt]
            else:
                df_copy = df_copy[df_copy['date'] <= end_dt]

        # 确保数据量足够（至少需要1天历史数据）
        if len(df_copy) < 1:
            return pd.Series([False] * len(df), index=df.index)

        # 日期关系：
        # latest_day: 最近一个交易日（最后一天）
        latest_day = df_copy.iloc[-1]
        
        # 检查换手率是否存在
        if '换手率' not in df_copy.columns:
            return pd.Series([False] * len(df), index=df.index)
        
        # 获取最近一个交易日换手率
        turnover_rate = latest_day['换手率']
        
        # 检查换手率是否大于5%且小于30%
        # 如果换手率值非常低（最大小于1），说明可能存在单位问题，尝试调整单位
        if df_copy['换手率'].max() < 1:
            # 假设换手率计算时成交量单位是手，需要乘以100转换为股
            adjusted_turnover_rate = turnover_rate * 100
            is_valid_turnover = (5 < adjusted_turnover_rate < 30)
        else:
            is_valid_turnover = (5 < turnover_rate < 30)
        
        # 创建结果序列
        result = pd.Series([False] * len(df), index=df.index)
        # 只在最后一天设置结果
        result.iloc[-1] = is_valid_turnover
        
        return result
    except Exception as e:
        print(f"换手率策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


def macdWeeklyGoldenCrossStrategy(df, start_date='', end_date='', mode=None):
    """
    MACD周线金叉策略：筛选出周线MACD指标出现金叉且在零轴上方的股票
    MACD金叉是指DIF线（快线）从下向上穿过DEA线（慢线）
    零轴上方意味着DIF线和DEA线都在零轴以上，代表股票处于多头市场
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
            # 设置日期为索引
            df_copy.set_index('date', drop=False, inplace=True)
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df_copy = df_copy[df_copy.index >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df_copy = df_copy[df_copy.index <= end_dt]

        # 确保数据量足够（至少需要20周数据，约100个交易日）
        if len(df_copy) < 100:
            return pd.Series([False] * len(df), index=df.index)
        
        # 将日线数据转换为周线数据
        # 周线数据的计算方式：
        # - 开盘价：每周第一个交易日的开盘价
        # - 最高价：每周所有交易日的最高价
        # - 最低价：每周所有交易日的最低价
        # - 收盘价：每周最后一个交易日的收盘价
        weekly_data = df_copy.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        
        # 确保周线数据量足够（至少需要26周数据，MACD计算需要）
        if len(weekly_data) < 26:
            return pd.Series([False] * len(df), index=df.index)
        
        # 计算周线MACD指标
        # 默认参数：fastperiod=12, slowperiod=26, signalperiod=9
        weekly_close = weekly_data['close']
        macd, macdsignal, macdhist = talib.MACD(weekly_close, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # 检查MACD数据是否有效
        if macd.isnull().all() or macdsignal.isnull().all():
            return pd.Series([False] * len(df), index=df.index)
        
        # MACD金叉是指DIF线（macd）从下向上穿过DEA线（macdsignal）
        # 计算前一天的DIF和DEA值
        prev_macd = macd.shift(1)
        prev_macdsignal = macdsignal.shift(1)
        
        # 生成金叉信号：当前DIF > 当前DEA 且 前一天DIF <= 前一天DEA，同时DIF和DEA都在零轴上方
        # 零轴上方条件：DIF > 0 且 DEA > 0
        golden_cross = (macd > macdsignal) & (prev_macd <= prev_macdsignal) & (macd > 0) & (macdsignal > 0)
        
        # 获取股票代码，用于调试
        stock_code = df_copy['code'].iloc[0] if 'code' in df_copy.columns else 'unknown'
        
        # 检查最近5周内是否出现过金叉
        recent_golden_cross = golden_cross.tail(5).any()
        
        # 简化逻辑：如果最近5周内出现过金叉，就认为符合条件
        if recent_golden_cross:
            # 对于快速模式，直接返回True
            if mode == 'fast':
                return True
            else:
                # 对于普通模式，返回完整序列，最后一天设为True
                final_result = pd.Series([False] * len(df), index=df.index)
                if len(final_result) > 0:
                    final_result.iloc[-1] = True
                return final_result
        else:
            # 没有金叉，返回False
            if mode == 'fast':
                return False
            else:
                return pd.Series([False] * len(df), index=df.index)
    except Exception as e:
        print(f"MACD周线金叉策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


def macdDailyGoldenCrossStrategy(df, start_date='', end_date='', mode=None):
    """
    MACD日线金叉策略：筛选出日线MACD指标出现金叉且在零轴上方的股票
    MACD金叉是指DIF线（快线）从下向上穿过DEA线（慢线）
    零轴上方意味着DIF线和DEA线都在零轴以上，代表股票处于多头市场
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
            # 设置日期为索引
            df_copy.set_index('date', drop=False, inplace=True)
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df_copy = df_copy[df_copy.index >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df_copy = df_copy[df_copy.index <= end_dt]

        # 确保数据量足够（至少需要26个交易日，MACD计算需要）
        if len(df_copy) < 26:
            return pd.Series([False] * len(df), index=df.index)
        
        # 计算日线MACD指标
        # 默认参数：fastperiod=12, slowperiod=26, signalperiod=9
        close_price = df_copy['close']
        macd, macdsignal, macdhist = talib.MACD(close_price, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # 检查MACD数据是否有效
        if macd.isnull().all() or macdsignal.isnull().all():
            return pd.Series([False] * len(df), index=df.index)
        
        # MACD金叉是指DIF线（macd）从下向上穿过DEA线（macdsignal）
        # 计算前一天的DIF和DEA值
        prev_macd = macd.shift(1)
        prev_macdsignal = macdsignal.shift(1)
        
        # 生成金叉信号：当前DIF > 当前DEA 且 前一天DIF <= 前一天DEA，同时DIF和DEA都在零轴上方
        # 零轴上方条件：DIF > 0 且 DEA > 0
        golden_cross = (macd > macdsignal) & (prev_macd <= prev_macdsignal) & (macd > 0) & (macdsignal > 0)
        
        # 获取股票代码，用于调试
        stock_code = df_copy['code'].iloc[0] if 'code' in df_copy.columns else 'unknown'
        
        # 检查最近5个交易日内是否出现过金叉
        recent_golden_cross = golden_cross.tail(5).any()
        
        # 调试信息
        # print(f"股票代码: {stock_code}，最近5个交易日金叉情况: {recent_golden_cross}, 总金叉数: {golden_cross.sum()}")
        
        # 简化逻辑：如果最近5个交易日内出现过金叉，就认为符合条件
        if recent_golden_cross:
            # 对于快速模式，直接返回True
            if mode == 'fast':
                return True
            else:
                # 对于普通模式，返回完整序列，最后一天设为True
                final_result = pd.Series([False] * len(df), index=df.index)
                if len(final_result) > 0:
                    final_result.iloc[-1] = True
                return final_result
        else:
            # 没有金叉，返回False
            if mode == 'fast':
                return False
            else:
                return pd.Series([False] * len(df), index=df.index)
    except Exception as e:
        print(f"MACD日线金叉策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


def continuousRiseWithNearHighStrategy(df, start_date='', end_date='', mode=None):
    """
    持续上升且接近最高价策略：
    1. 最近三个交易日的收盘价都是持续上升
    2. 每个交易日的收盘价和当天的最高价偏差在1个点以内（即收盘价 >= 最高价 * 0.99）
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index >= start_dt]
            else:
                df_copy = df_copy[df_copy['date'] >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            if isinstance(df_copy.index, pd.DatetimeIndex):
                df_copy = df_copy[df_copy.index <= end_dt]
            else:
                df_copy = df_copy[df_copy['date'] <= end_dt]

        # 确保数据量足够（至少需要3个交易日）
        if len(df_copy) < 3:
            return pd.Series([False] * len(df), index=df.index)

        # 获取最近3个交易日的数据
        recent_data = df_copy.tail(3)
        
        # 检查收盘价、最高价是否存在
        if 'close' not in recent_data.columns or 'high' not in recent_data.columns:
            return pd.Series([False] * len(df), index=df.index)
        
        # 条件1：最近三个交易日的收盘价都是持续上升
        # 收盘价1 < 收盘价2 < 收盘价3
        close_prices = recent_data['close'].values
        is_continuous_rise = (close_prices[0] < close_prices[1]) and (close_prices[1] < close_prices[2])
        
        # 条件2：每个交易日的收盘价和当天的最高价偏差在1个点以内
        # 收盘价 >= 最高价 * 0.99
        close_high_pairs = list(zip(recent_data['close'], recent_data['high']))
        is_near_high = all(close >= high * 0.99 for close, high in close_high_pairs)
        
        # 合并条件
        final_result = is_continuous_rise and is_near_high
        
        # 创建结果序列
        result = pd.Series([False] * len(df), index=df.index)
        # 只在最后一天设置结果
        result.iloc[-1] = final_result
        
        return result
    except Exception as e:
        print(f"持续上升且接近最高价策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


@register_strategy("MACD周线区间")
def macdWeeklyRangeStrategy(df, start_date='', end_date='', mode=None):
    """
    周线MACD区间策略：筛选出最近一周的MACD、DIF、DEA同时在0.05到0.3之间，且最近2周以上MACD值逐步放大的股票
    :param df: DataFrame格式，具体一个股票的数据表
    :param start_date: 可选，开始日期，格式"2020-10-10"
    :param end_date: 可选，结束日期，格式"2020-10-10"
    :param mode: str类型，'fast'为快速模式，用于盘中选股，获取实时行情数据
    :return: 布尔序列，每个元素对应一个交易日的选股结果
    """
    try:
        # 处理date既是索引又是列的情况
        # 创建一个副本，避免修改原数据
        df_copy = df.copy()
        
        # 确保数据按日期排序
        if isinstance(df_copy.index, pd.DatetimeIndex):
            # 如果索引是日期类型，直接排序
            df_copy = df_copy.sort_index()
        else:
            # 否则，使用date列排序
            df_copy = df_copy.sort_values('date')
            # 设置日期为索引
            df_copy.set_index('date', drop=False, inplace=True)
        
        # 处理日期筛选
        if start_date:
            start_dt = pd.to_datetime(start_date)
            df_copy = df_copy[df_copy.index >= start_dt]
        if end_date:
            end_dt = pd.to_datetime(end_date)
            df_copy = df_copy[df_copy.index <= end_dt]

        # 确保数据量足够（至少需要20周数据，约100个交易日）
        if len(df_copy) < 100:
            return pd.Series([False] * len(df), index=df.index)
        
        # 将日线数据转换为周线数据
        weekly_data = df_copy.resample('W').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        
        # 确保周线数据量足够（至少需要26周数据，MACD计算需要，且需要最近2周以上的数据）
        if len(weekly_data) < 28:  # 26周MACD计算 + 2周比较
            return pd.Series([False] * len(df), index=df.index)
        
        # 计算周线MACD指标
        # 默认参数：fastperiod=12, slowperiod=26, signalperiod=9
        weekly_close = weekly_data['close']
        # 注意：talib.MACD返回的是 (macdhist, macd, macdsignal) = talib.MACD(close)
        # 其中：macdhist是柱状图，macd是DIF线，macdsignal是DEA线
        macdhist, dif, dea = talib.MACD(weekly_close, fastperiod=12, slowperiod=26, signalperiod=9)
        
        # 检查MACD数据是否有效
        if dif.isnull().all() or dea.isnull().all() or macdhist.isnull().all():
            return pd.Series([False] * len(df), index=df.index)
        
        # 获取最近3周的数据（需要比较最近2周是否逐步放大）
        recent_dif = dif.tail(3)
        recent_dea = dea.tail(3)
        recent_macdhist = macdhist.tail(3)
        
        # 检查最近一周的dif、dea、macdhist是否都在0.05到0.3之间
        # 最近一周是tail(3)[-1]，即索引为-1
        current_week_valid = (
            (recent_dif.iloc[-1] > 0.05) & (recent_dif.iloc[-1] < 0.3) &
            (recent_dea.iloc[-1] > 0.05) & (recent_dea.iloc[-1] < 0.3) &
            (recent_macdhist.iloc[-1] > 0.05) & (recent_macdhist.iloc[-1] < 0.3)
        )
        
        # 检查最近2周以上macd值是否逐步放大
        # 最近2周是tail(3)[1:3]，即索引为1,2
        macd_increasing = (
            (recent_macdhist.iloc[1] < recent_macdhist.iloc[2])
        )
        
        # 获取股票代码，用于调试
        stock_code = df_copy['code'].iloc[0] if 'code' in df_copy.columns else 'unknown'
        
        # 调试信息
        # print(f"股票代码: {stock_code}, 当前周有效: {current_week_valid}, MACD逐步放大: {macd_increasing}")
        
        # 如果满足条件
        if current_week_valid and macd_increasing:
            # 对于快速模式，直接返回True
            if mode == 'fast':
                return True
            else:
                # 对于普通模式，返回完整序列，最后一天设为True
                final_result = pd.Series([False] * len(df), index=df.index)
                if len(final_result) > 0:
                    final_result.iloc[-1] = True
                return final_result
        else:
            # 不满足条件，返回False
            if mode == 'fast':
                return False
            else:
                return pd.Series([False] * len(df), index=df.index)
    except Exception as e:
        print(f"周线MACD区间策略异常：{e}")
        # 如果发生任何异常，返回一个与df索引相同长度的False序列
        return pd.Series([False] * len(df), index=df.index)


# 初始化策略列表
strategy_list = [
    {
        "name": "hs300Signal",
        "function": hs300SignalStrategy,
        "type": "market_env",
        "description": "HS300信号策略，判断当日是否适合买入股票"
    },
    {
        "name": "stockSelection",
        "function": stockSelectionStrategy,
        "type": "selection",
        "description": "选股策略，从股票池中筛选出符合特定条件的股票"
    },
    {
        "name": "buySignal",
        "function": buySignalStrategy,
        "type": "buy",
        "description": "买入信号策略，结合多种指标判断个股是否适合买入"
    },
    {
        "name": "sellSignal",
        "function": sellSignalStrategy,
        "type": "sell",
        "description": "卖出信号策略，根据风险情况和持有时间判断是否卖出"
    },
    {
        "name": "highOpenLimitUp",
        "function": highOpenLimitUpStrategy,
        "type": "selection",
        "description": "高开涨停策略，筛选上一交易日涨停且今日高开2%的股票"
    },
    {
        "name": "prevDayLimitUp",
        "function": prevDayLimitUpStrategy,
        "type": "selection",
        "description": "上一交易日涨停策略，筛选上一交易日涨停的股票，包含ST股和创业板"
    },
    {
        "name": "todayLimitUp",
        "function": todayLimitUpStrategy,
        "type": "selection",
        "description": "今日涨停策略，筛选今日涨停的股票，包含ST股和创业板"
    },
    {
        "name": "smallMarketCap",
        "function": smallMarketCapStrategy,
        "type": "selection",
        "description": "小市值策略，筛选流通市值小于100亿的股票"
    },
    {
        "name": "turnoverRateStrategy",
        "function": turnoverRateStrategy,
        "type": "selection",
        "description": "换手率策略，筛选最近一个交易日（如果今天是交易日就是今日）换手率大于5%且小于30%的股票"
    },
    {
        "name": "macdDailyGoldenCross",
        "function": macdDailyGoldenCrossStrategy,
        "type": "selection",
        "description": "MACD日线金叉策略，筛选出日线MACD指标出现金叉且在零轴上方的股票"
    },
    {
        "name": "macdWeeklyGoldenCross",
        "function": macdWeeklyGoldenCrossStrategy,
        "type": "selection",
        "description": "MACD周线金叉策略，筛选出周线MACD指标出现金叉且在零轴上方的股票"
    },
    {
        "name": "continuousRiseWithNearHigh",
        "function": continuousRiseWithNearHighStrategy,
        "type": "selection",
        "description": "持续上升且接近最高价策略，筛选最近三个交易日收盘价持续上升且每个交易日收盘价与最高价偏差在1个点以内的股票"
    },
    {
        "name": "macdWeeklyRange",
        "function": macdWeeklyRangeStrategy,
        "type": "selection",
        "description": "周线MACD区间策略，筛选最近一周MACD、DIF、DEA同时大于0.05小于0.2且最近3周以上MACD值逐步放大的股票"
    }
]


# 策略枚举说明：执行xuangu.py时可用的策略参数对应的策略名称和功能说明
# 例如：python xuangu.py 10 执行持续上升且接近最高价策略
# 可通过多个数字组合执行多个策略，例如：python xuangu.py 1 10
# 执行所有策略可直接运行：python xuangu.py
STRATEGY_ENUM = {
    '1': '选股策略 - 从股票池中筛选出符合特定条件的股票',
    '2': '买入信号策略 - 结合多种指标判断个股是否适合买入',
    '3': '高开涨停策略 - 筛选上一交易日涨停且今日高开2%的股票',
    '4': '上一交易日涨停策略 - 筛选上一交易日涨停的股票，包含ST股和创业板',
    '5': '今日涨停策略 - 筛选今日涨停的股票，包含ST股和创业板',
    '6': '小市值策略 - 筛选流通市值小于100亿的股票',
    '7': '换手率策略 - 筛选最近一个交易日换手率大于5%且小于30%的股票',
    '8': 'MACD周线金叉策略 - 筛选周线MACD指标出现金叉且在零轴上方的股票',
    '9': 'MACD日线金叉策略 - 筛选日线MACD指标出现金叉且在零轴上方的股票',
    '10': '持续上升且接近最高价策略 - 筛选最近三个交易日收盘价持续上升且每个交易日收盘价与最高价偏差在1个点以内的股票',
    '11': '周线MACD区间策略 - 筛选最近一周MACD、DIF、DEA同时大于0.05小于0.2且最近3周以上MACD值逐步放大的股票'
}


if __name__ == '__main__':
    # 调试用代码. 此文件不直接执行。通过xuangu.py或celue_save.py调用
    
    stock_code = '000887'
    start_date = ''
    end_date = ''
    df_stock = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + stock_code + '.csv',
                           index_col=None, encoding='gbk', dtype={'code': str})
    df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
    df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并

    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    try:
        if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00' and is_trading_day():
            df_today = func.get_tdx_lastestquote((1, '000300'))
            df_hs300 = func.update_stockquote('000300', df_hs300, df_today)
    except ModuleNotFoundError:
        print('pytdx模块未安装，跳过实时行情更新')
    hs300_signal = hs300SignalStrategy(df_hs300)

    if not hs300_signal.iat[-1]:
        print('今日HS300不满足买入条件，停止选股')

    try:
        if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00' and is_trading_day():
            df_today = func.get_tdx_lastestquote(stock_code)
            df_stock = func.update_stockquote(stock_code, df_stock, df_today)
    except ModuleNotFoundError:
        print('pytdx模块未安装，跳过实时行情更新')
    
    # 测试选股策略
    stock_selection_fast = stockSelectionStrategy(df_stock, mode='fast', start_date=start_date, end_date=end_date)
    stock_selection = stockSelectionStrategy(df_stock, mode='', start_date=start_date, end_date=end_date)
    
    # 测试买入信号策略
    buy_signal = buySignalStrategy(df_stock, hs300_signal, start_date=start_date, end_date=end_date)
    
    # 测试高开涨停策略
    high_open_limit_up = highOpenLimitUpStrategy(df_stock, start_date=start_date, end_date=end_date)
    
    # 测试上一交易日涨停策略
    prev_day_limit_up = prevDayLimitUpStrategy(df_stock, start_date=start_date, end_date=end_date)
    
    # 测试卖出信号策略
    sell_signal = sellSignalStrategy(df_stock, buy_signal, start_date=start_date, end_date=end_date)
    
    # 输出测试结果
    print(f'{stock_code} 选股策略(快速):{stock_selection_fast} 选股策略:{stock_selection.iat[-1]} ')
    print(f'{stock_code} 买入信号策略:{buy_signal.iat[-1]} 高开涨停策略:{high_open_limit_up.iat[-1]} 上一交易日涨停策略:{prev_day_limit_up.iat[-1]} ')
    print(f'{stock_code} 卖出信号策略:{sell_signal.iat[-1]}')
    
    # 打印策略列表信息
    print(f'\n当前策略列表:')
    for strategy in strategy_list:
        print(f"策略名称: {strategy['name']}, 类型: {strategy['type']}, 描述: {strategy['description']}")
