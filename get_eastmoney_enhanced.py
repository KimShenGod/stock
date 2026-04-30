import requests
import time
import pandas as pd
import re
import akshare as ak
from datetime import datetime,timedelta

MARKET_PREFIX_MAP = {
    'sh': '1',  # 沪市
    'sz': '0',  # 深市
    'bj': '0',  # 北交所（与深市共用前缀，需注意代码规则）
    'hk': '116', # 港股
    'us': '105'  # 美股
}

STOCK_CODE_RULES = {
    '0': ['000', '001', '002', '300', '301'],  # 深市：000开头(主板), 002开头(中小板), 300开头(创业板)
    '1': ['600', '601', '603', '605'],        # 沪市：600/601/603/605开头
    '116': ['hk'],                             # 港股：hk开头
    '105': ['us']                              # 美股：us开头
}

def get_market_code(stock_code):
    """
    根据股票代码自动判断市场代码
    :param stock_code: 股票代码，可以是纯数字或带市场前缀
    :return: 市场代码
    """
    # 标准化处理：去除空格，转为小写
    code = stock_code.strip().lower()
    
    # 处理各种格式的股票代码
    # 格式1: sz.002331, sh.600000
    if '.' in code:
        parts = code.split('.')
        if len(parts) == 2:
            prefix = parts[0]
            number_code = parts[1]
            if prefix in MARKET_PREFIX_MAP:
                return MARKET_PREFIX_MAP[prefix]
            # 反向格式: 002331.sz, 600000.sh
            elif parts[1] in MARKET_PREFIX_MAP:
                return MARKET_PREFIX_MAP[parts[1]]
    
    # 格式2: sz002331, sh600000
    for prefix, market in MARKET_PREFIX_MAP.items():
        if code.startswith(prefix):
            return market
        # 格式3: 002331sz, 600000sh
        elif code.endswith(prefix):
            return market
    
    # 提取纯数字部分
    import re
    number_match = re.search(r'\d+', code)
    if number_match:
        number_code = number_match.group()
        # 根据数字前缀判断
        if len(number_code) >= 3:
            code_prefix = number_code[:3]
            for market, prefixes in STOCK_CODE_RULES.items():
                if code_prefix in prefixes:
                    return market
    
    # 默认返回深市
    return '0'


def get_stock_name(stock_code):
    """
    获取股票名称
    :param stock_code: 股票代码，如'000001'
    :return: 股票名称
    """
    # 1. 获取所有A股代码和名称的映射表
    stock_code_name_df = ak.stock_info_a_code_name()
    # 3. 创建一个字典以便快速查找，提升后续查询速度
    code_to_name = dict(zip(stock_code_name_df['code'], stock_code_name_df['name']))
    return code_to_name.get(stock_code, f"未找到股票 {stock_code} 的名称")
    

def get_intraday_data(stock_code, market=None, ndays=1):
    """
    获取东方财富分时数据
    :param stock_code: 股票代码，如'000001'
    :param market: 市场代码，'0'=深市，'1'=沪市，'116'=港股等，None表示自动判断
    :param ndays: 最近几天，通常1为当日
    :return: DataFrame
    """
    # 自动判断市场代码
    if market is None:
        market = get_market_code(stock_code)
        print(f"自动判断市场代码: {market}")
    
    url = "http://push2.eastmoney.com/api/qt/stock/trends2/get"
    
    # 生成13位毫秒级时间戳
    timestamp = int(time.time() * 1000)
    
    params = {
        "secid": f"{market}.{stock_code}",
        "fields1": "f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "iscr": "0",       # 0:不含集合竞价, 1:含集合竞价
        "ndays": str(ndays),
        "ut": "fa5fd1943c7b386f172d6893dbfba10b", # 通用令牌
        "_": timestamp     # 动态时间戳，防止缓存和过期
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "http://quote.eastmoney.com/"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        if data['data'] is None:
            print(f"未找到股票 {stock_code} 的分时数据")
            return pd.DataFrame()
            
        trends = data['data']['trends']
        if not trends:
            return pd.DataFrame()
            
        # 打印原始数据格式
        print("原始数据示例:")
        for i, item in enumerate(trends[:3]):
            print(f"{i+1}. {item}")
        
        # 解析数据 - 修正列映射
        # 实际数据结构: [time, open, close, high, low, volume, amount, avg_price]
        columns = ['time', 'open', 'close', 'high', 'low', 'volume', 'amount', 'avg_price']
        df_list = [item.split(',') for item in trends]
        df = pd.DataFrame(df_list, columns=columns)
        
        # 打印解析后的数据
        print("\n解析后的数据:")
        print(df.head())
        
        # 数据类型转换
        df['open'] = df['open'].astype(float)
        df['close'] = df['close'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['volume'] = df['volume'].astype(int)
        df['amount'] = df['amount'].astype(float)
        df['avg_price'] = df['avg_price'].astype(float)
        
        return df
        
    except Exception as e:
        print(f"请求失败: {e}")
        return pd.DataFrame()

def get_kline_data(stock_code, market=None, klt=101, fqt=1, beg='0', end='20500101'):
    """
    获取东方财富K线数据
    :param stock_code: 股票代码，如'000001'
    :param market: 市场代码，'0'=深市，'1'=沪市，'116'=港股等，None表示自动判断
    :param klt: K线周期，101=日线, 102=周线, 103=月线, 1=1分钟, 5=5分钟, 15=15分钟, 30=30分钟, 60=60分钟
    :param fqt: 复权类型，0=不复权, 1=前复权, 2=后复权
    :param beg: 开始日期，格式为YYYYMMDD，0表示从上市开始
    :param end: 结束日期，格式为YYYYMMDD
    :return: DataFrame
    """
    # 自动判断市场代码
    if market is None:
        market = get_market_code(stock_code)
        print(f"自动判断市场代码: {market}")
    
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    
    # 生成13位毫秒级时间戳
    timestamp = int(time.time() * 1000)
    
    params = {
        "secid": f"{market}.{stock_code}",
        "fields1": "f1,f2,f3,f4,f5,f6",  # 基础信息字段
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",  # K线数据字段
        "klt": str(klt),     # K线周期
        "fqt": str(fqt),     # 复权类型
        "beg": beg,          # 开始日期
        "end": end,          # 结束日期
        "ut": "fa5fd1943c7b386f172d6893dbfba10b", # 通用令牌
        "_": timestamp       # 动态时间戳，防止缓存和过期
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": "http://quote.eastmoney.com/"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=5)
        response.raise_for_status()
        data = response.json()
        
        if data['data'] is None:
            print(f"未找到股票 {stock_code} 的K线数据")
            return pd.DataFrame()
            
        klines = data['data']['klines']
        if not klines:
            return pd.DataFrame()
            
        # 打印原始数据格式
        print("K线原始数据示例:")
        for i, item in enumerate(klines[:3]):
            print(f"{i+1}. {item}")
        
        # 解析数据
        # K线数据结构: [date, open, close, high, low, volume, amount, amplitude, pct_change, change, turnover_rate]
        columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'pct_change', 'change', 'turnover_rate']
        df_list = [item.split(',') for item in klines]
        # 确保只取需要的列
        df_list = [item[:len(columns)] for item in df_list]
        df = pd.DataFrame(df_list, columns=columns)
        
        # 打印解析后的数据
        print("\n解析后的数据:")
        print(df.tail())
        
        # 数据类型转换
        numeric_columns = ['open', 'close', 'high', 'low', 'volume', 'amount', 'amplitude', 'pct_change', 'change', 'turnover_rate']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
        
    except Exception as e:
        print(f"请求失败: {e}")
        return pd.DataFrame()

def get_stock_news(stock_code, market=None, page=1, size=100):
    """
    获取东方财富股票新闻资讯（使用akshare）
    :param stock_code: 股票代码，如'000001'
    :param market: 市场代码，'0'=深市，'1'=沪市，'116'=港股等，None表示自动判断
    :param page: 页码，从1开始
    :param size: 每页数量，默认100
    :return: DataFrame
    """
    try:
        import re
        
        # 使用akshare获取新闻数据
        news_df = ak.stock_news_em(symbol=stock_code)
        
        if news_df.empty:
            stock_name = get_stock_name(stock_code)
            print(f"未找到股票 {stock_name}({stock_code}) 的新闻数据")
            return pd.DataFrame()
        
        # 使用get_stock_name函数获取股票名称
        stock_name = get_stock_name(stock_code)
        print(f"股票名称: {stock_name}")
        
        # 筛选标题包含股票代码或股票名称的新闻
        # 匹配格式：002331、002331.SZ、002331.SZ)、SZ002331、皖通科技等
        stock_code_patterns = [
            stock_code,                           # 纯数字代码：002331
            f"{stock_code}\\.SZ",                 # 带后缀：002331.SZ
            f"{stock_code}\\.SH",                 # 带后缀：002331.SH
            f"{stock_code}\\(",                   # 带左括号：002331(
            f"SZ{stock_code}",                   # 带前缀：SZ002331
            f"SH{stock_code}",                   # 带前缀：SH002331
            f"SZ\\.{stock_code}",                # 带交易所前缀：SZ.002331
            f"SH\\.{stock_code}",                # 带交易所前缀：SH.002331
        ]
        
        # 如果获取到股票名称，添加名称匹配模式
        if stock_name != stock_code:
            # 股票名称可能包含特殊字符，需要转义
            escaped_name = re.escape(stock_name)
            stock_code_patterns.append(escaped_name)
            print(f"添加股票名称到筛选条件: {stock_name}")
        
        # 构建正则表达式
        pattern = '|'.join(stock_code_patterns)
        mask = news_df['新闻标题'].str.contains(pattern, case=False, na=False, regex=True)
        news_df_filtered = news_df.loc[mask].copy()
        
        if news_df_filtered.empty:
            print(f"筛选后未找到股票 {stock_code} 的相关新闻")
            return pd.DataFrame()
        
        print(f"筛选后相关新闻条数: {len(news_df_filtered)}")
        
        # 按发布时间降序排序
        if '发布时间' in news_df_filtered.columns:
            news_df_filtered['发布时间'] = pd.to_datetime(news_df_filtered['发布时间'])
            news_df_filtered = news_df_filtered.sort_values('发布时间', ascending=False)
        
        # 限制返回数量
        if size > 0:
            news_df_filtered = news_df_filtered.head(size)
        
        # 打印解析后的数据
        print("\n解析后的数据:")
        print(f"总新闻条数: {len(news_df_filtered)}")
        print(news_df_filtered.head())
        
        return news_df_filtered
        
    except Exception as e:
        print(f"请求失败: {e}")
        return pd.DataFrame()


print("\n=== 测试 002331 (皖通科技) 新闻资讯 ===")
df_002331_news = get_stock_news('002333')
if not df_002331_news.empty:
    for index, row in df_002331_news.iterrows():
        print(f"标题: {row.get('新闻标题', 'N/A')}")
        print(f"时间: {row.get('发布时间', 'N/A')}")
        print(f"来源: {row.get('文章来源', 'N/A')}")
        content = str(row.get('新闻内容', 'N/A'))
        print(f"内容: {content}")
        print(f"链接: {row.get('新闻链接', 'N/A')}")
        print("-" * 50)
else:
    print("未获取到新闻数据")


def get_intraday_sina_safe(symbol):
    """安全获取股票最近一个交易日分钟级行情，带空数据检测"""
    date = datetime.now().strftime("%Y%m%d")
    # 获取从1990年至今的所有A股交易日
    trade_dates_df = ak.tool_trade_date_hist_sina()
    # 转换日期格式为字符串，便于比较
    trade_dates = trade_dates_df["trade_date"].astype(str).tolist()
    
    # 获取今天的日期
    today = datetime.now().date()
    
    # 从今天开始向前查找，直到找到第一个交易日
    while True:
        today_str = today.strftime("%Y-%m-%d")
        if today_str in trade_dates:
            today_str = today_str.replace("-", "")
            date = today_str
            break
        # 如果今天不是交易日，则向前推一天
        today -= timedelta(days=1)
    try:
        df = ak.stock_intraday_sina(symbol=symbol, date=date)
        if df is None or df.empty:
            print(f"⚠️ 警告: {symbol} 在 {date} 没有数据（可能是非交易日或未来日期）")
            return None
        return df
    except KeyError as e:
        print(f"⚠️ 警告: 接口返回异常（列缺失: {e}），通常是因为无数据。日期={date}")
        return None
    except Exception as e:
        print(f"❌ 请求失败: {e}")
        return None

# 使用示例
symbol = "sz002331"
df = get_intraday_sina_safe(symbol=symbol)
if df is not None:
    print(df)
