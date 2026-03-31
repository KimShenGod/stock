"""
选股多线程版本文件。导入数据——执行策略——显示结果
为保证和通达信选股一致，需使用前复权数据
"""
import os
import sys
import time
import pandas as pd
from multiprocessing import Pool, RLock, freeze_support
from rich import print
from tqdm import tqdm
import CeLue  # 个人策略文件，不分享
import func
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

# 配置部分

start_date = ''
end_date = ''

# 变量定义
tdxpath = ucfg.tdx['tdx_path']
csvdaypath = ucfg.tdx['pickle']
已选出股票列表 = []  # 策略选出的股票
要剔除的通达信概念 = ["ST板块", ]  # list类型。通达信软件中查看“概念板块”。
要剔除的通达信行业 = ["T1002", ]  # list类型。记事本打开 通达信目录\incon.dat，查看#TDXNHY标签的行业代码。
# 股票代码到名称的映射字典
stock_name_dict = {}

starttime_str = time.strftime("%H:%M:%S", time.localtime())
starttime = time.time()
starttime_tick = time.time()


def make_stocklist():
    # 要进行策略的股票列表筛选
    stocklist = [i[:-4] for i in os.listdir(ucfg.tdx['csv_lday'])]  # 去文件名里的.csv，生成纯股票代码list
    print(f'生成股票列表, 共 {len(stocklist)} 只股票')
    
    # 简化版本：只剔除科创板股票，跳过需要pytdx的ST板块剔除和行业剔除
    print("剔除科创板股票")
    tmplist = []
    for stockcode in stocklist:
        if stockcode[:2] != '68':
            tmplist.append(stockcode)
    stocklist = tmplist
    
    # 构建股票代码到名称的映射字典
    print("构建股票代码到名称的映射字典")
    
    # 优先尝试从指定URL获取股票名称
    print("\n优先尝试从巨潮资讯网获取股票名称...")
    
    url_matched = 0
    url_success = False
    
    try:
        import requests
        import json
        
        # 初始化股票数据列表
        all_stocks = []
        
        # 获取巨潮资讯网股票数据（szse_stock.json已包含深交所和上交所的所有股票数据）
        url = "https://www.cninfo.com.cn/new/data/szse_stock.json"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        data = json.loads(response.text)
        all_stocks = data.get('stockList', [])
        print(f'成功获取巨潮资讯网股票数据，共 {len(all_stocks)} 只股票')
        
        # 构建股票代码到名称的映射
        for stock in all_stocks:
            stockcode = stock.get('code', '')
            # 巨潮资讯网返回的股票名称字段是'zwjc'，而不是'name'
            stock_name = stock.get('zwjc', '')
            if stockcode and stock_name and stockcode in stocklist and stockcode not in stock_name_dict:
                stock_name_dict[stockcode] = stock_name
                url_matched += 1
        
        print(f'从巨潮资讯网获取到 {url_matched} 只股票的名称')
        url_success = True
    except Exception as url_e:
        print(f'从巨潮资讯网获取股票名称失败: {url_e}，尝试从akshare获取')
    
    # 只有当从巨潮资讯网获取的股票名称不足时，才尝试从akshare获取
    if url_success and url_matched >= len(stocklist):
        print(f"\n已从巨潮资讯网获取到足够的股票名称（{url_matched} 只），跳过akshare获取步骤")
    else:
        # 尝试从akshare获取股票名称
        print("\n尝试使用akshare库获取股票名称...")
        
        try:
            import akshare as ak
            
            # 尝试使用不同的akshare方法获取股票列表
            try:
                # 方法0: 尝试使用stock_info_a_code_name()方法
                stock_basic = ak.stock_info_a_code_name()
                stock_basic.rename(columns={'code': 'symbol', 'name': 'name'}, inplace=True)
                print(f'使用 stock_info_a_code_name 方法获取股票列表成功')
            except Exception as e:
                print(f'stock_info_a_code_name 方法失败: {e}，尝试其他方法')
                try:
                    # 方法1: 尝试获取所有A股股票列表
                    stock_basic = ak.stock_zh_a_spot_em()
                    stock_basic.rename(columns={'代码': 'symbol', '名称': 'name'}, inplace=True)
                    print(f'使用 stock_zh_a_spot_em 方法获取股票列表成功')
                except Exception as e:
                    print(f'stock_zh_a_spot_em 方法失败: {e}，尝试其他方法')
                    # 方法2: 尝试获取上海A股列表
                    stock_basic = ak.stock_info_sh_name_code()
                    stock_basic.rename(columns={'代码': 'symbol', '名称': 'name'}, inplace=True)
                    # 方法3: 尝试获取深圳A股列表并合并
                    stock_sz = ak.stock_info_sz_name_code()
                    stock_sz.rename(columns={'A股代码': 'symbol', 'A股简称': 'name'}, inplace=True)
                    stock_basic = pd.concat([stock_basic, stock_sz], ignore_index=True)
                    print(f'使用 stock_info_sh_name_code 和 stock_info_sz_name_code 方法获取股票列表成功')
            
            # 构建股票代码到名称的映射
            akshare_matched = 0
            for _, row in stock_basic.iterrows():
                stockcode = row['symbol']
                stock_name = row['name']
                if stockcode in stocklist and stockcode not in stock_name_dict:
                    stock_name_dict[stockcode] = stock_name
                    akshare_matched += 1
            
            print(f'从akshare获取到 {akshare_matched} 只股票的名称')
        except Exception as e:
            print(f'从akshare获取股票名称失败: {e}，继续执行')
    
    # 无论是否从akshare或URL获取成功，都尝试从本地文件获取，以补充缺失的股票名称
    print("\n尝试从本地文件获取股票名称，补充缺失的股票...")
    
    # 1. 尝试从csv文件中获取股票名称
    csv_path = ucfg.tdx['csv_lday']
    local_matched = 0
    
    for stockcode in stocklist:
        if stockcode not in stock_name_dict:  # 只处理尚未获取名称的股票
            try:
                # 尝试从csv文件获取名称
                csv_file = csv_path + os.sep + stockcode + '.csv'
                if os.path.exists(csv_file):
                    # 只读取第一行，获取股票名称
                    df = pd.read_csv(csv_file, nrows=1, encoding='gbk')
                    if 'name' in df.columns:
                        stock_name = df['name'].iloc[0]
                        stock_name_dict[stockcode] = stock_name
                        local_matched += 1
                        continue
            except Exception as e:
                # 忽略错误，继续尝试其他方式
                pass
            
            try:
                # 尝试从pickle文件获取名称
                pklfile = csvdaypath + os.sep + stockcode + '.pkl'
                if os.path.exists(pklfile):
                    df_stock = pd.read_pickle(pklfile)
                    if 'name' in df_stock.columns:
                        stock_name = df_stock['name'].iloc[0]
                        stock_name_dict[stockcode] = stock_name
                        local_matched += 1
            except Exception as e:
                # 忽略错误，继续处理其他股票
                pass
    
    if local_matched > 0:
        print(f'从本地文件获取到 {local_matched} 只股票的名称')
    
    print(f'成功构建 {len(stock_name_dict)} 只股票的名称映射')
    
    # 打印部分匹配结果，用于调试
    print(f"\n部分匹配结果示例:")
    sample_count = 0
    for stockcode in stock_name_dict:
        if sample_count < 10:
            print(f"  {stockcode} - {stock_name_dict[stockcode]}")
            sample_count += 1
        else:
            break
    
    return stocklist


def load_dict_stock(stocklist):
    dicttemp = {}
    starttime_tick = time.time()
    tq = tqdm(stocklist)
    for stockcode in tq:
        tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        # dict[stockcode] = pd.read_csv(csvfile, encoding='gbk', index_col=None, dtype={'code': str})
        dicttemp[stockcode] = pd.read_pickle(pklfile)
    print(f'载入完成 用时 {(time.time() - starttime_tick):.2f} 秒')
    return dicttemp


def run_celue1(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
            
            celue1 = CeLue.stockSelectionStrategy(df_stock, start_date=start_date, end_date=end_date, mode='fast')
            if celue1:
                valid_stocklist.append(stockcode)
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue2(stocklist, HS300_信号, df_gbbq, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
            
            if df_today_filtered is not None and '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00' \
                    and is_trading_day():
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_code = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_code.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_code)
                        # 判断今天是否在该股的权息日内。如果是，需要重新前复权
                        now_date = pd.to_datetime(time.strftime("%Y-%m-%d", time.localtime()))
                        if now_date in df_gbbq.loc[df_gbbq['code'] == stockcode]['权息日'].to_list():
                            cw_dict = func.readall_local_cwfile()
                            df_stock = func.make_fq(stockcode, df_stock, df_gbbq, cw_dict)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            try:
                celue2_result = CeLue.buySignalStrategy(df_stock, HS300_信号, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue2_result, pd.Series) and len(celue2_result) > 0:
                    celue2 = celue2_result.iat[-1]
                    if celue2:
                        valid_stocklist.append(stockcode)
                else:
                    print(f'{stockcode} 买入信号策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 策略2结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 策略2结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 策略2执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue3(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue3_result = CeLue.highOpenLimitUpStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue3_result, pd.Series) and len(celue3_result) > 0:
                    celue3 = celue3_result.iat[-1]
                    if celue3:
                        valid_stocklist.append(stockcode)
                else:
                    print(f'{stockcode} 高开涨停策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 高开涨停策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 高开涨停策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 高开涨停策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue4(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue4_result = CeLue.prevDayLimitUpStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue4_result, pd.Series) and len(celue4_result) > 0:
                    celue4 = celue4_result.iat[-1]
                    if celue4:
                        valid_stocklist.append(stockcode)
                else:
                    print(f'{stockcode} 上一交易日涨停策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 上一交易日涨停策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 上一交易日涨停策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 上一交易日涨停策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue5(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue5_result = CeLue.todayLimitUpStrategy(df_stock, start_date=start_date, end_date=end_date, mode='fast')
                # 检查结果是否为Series类型且不为空
                if isinstance(celue5_result, pd.Series) and len(celue5_result) > 0:
                    celue5 = celue5_result.iat[-1]
                    if celue5:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue5_result, bool) and celue5_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} 今日涨停策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 今日涨停策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 今日涨停策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 今日涨停策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue6(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue6_result = CeLue.smallMarketCapStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue6_result, pd.Series) and len(celue6_result) > 0:
                    celue6 = celue6_result.iat[-1]
                    if celue6:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue6_result, bool) and celue6_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} 小市值策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 小市值策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 小市值策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 小市值策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue7(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue7_result = CeLue.turnoverRateStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue7_result, pd.Series) and len(celue7_result) > 0:
                    celue7 = celue7_result.iat[-1]
                    if celue7:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue7_result, bool) and celue7_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} 换手率策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 换手率策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 换手率策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 换手率策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue8(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue8_result = CeLue.macdWeeklyGoldenCrossStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue8_result, pd.Series) and len(celue8_result) > 0:
                    celue8 = celue8_result.iat[-1]
                    if celue8:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue8_result, bool) and celue8_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} MACD周线金叉策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} MACD周线金叉策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} MACD周线金叉策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} MACD周线金叉策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue9(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue9_result = CeLue.macdDailyGoldenCrossStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue9_result, pd.Series) and len(celue9_result) > 0:
                    celue9 = celue9_result.iat[-1]
                    if celue9:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue9_result, bool) and celue9_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} MACD日线金叉策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} MACD日线金叉策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} MACD日线金叉策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} MACD日线金叉策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue10(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue10_result = CeLue.continuousRiseWithNearHighStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue10_result, pd.Series) and len(celue10_result) > 0:
                    celue10 = celue10_result.iat[-1]
                    if celue10:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue10_result, bool) and celue10_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} 持续上升且接近最高价策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 持续上升且接近最高价策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 持续上升且接近最高价策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 持续上升且接近最高价策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


def run_celue11(stocklist, df_today, tqdm_position=None):
    if 'single' in sys.argv[1:]:
        tq = tqdm(stocklist[:])
    else:
        # 多进程模式下不使用tqdm，避免死锁
        tq = stocklist[:]
    
    # 创建一个新列表来存储有效的股票代码，避免在迭代中修改列表
    valid_stocklist = []
    
    # 提前筛选df_today中当前股票列表的数据，避免在循环中重复筛选
    df_today_filtered = df_today[df_today['code'].isin(stocklist)] if df_today is not None and not df_today.empty else None
    
    for stockcode in tq:
        # 只有单进程模式下才更新进度条描述
        if isinstance(tq, tqdm):
            tq.set_description(stockcode)
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        
        try:
            # 检查文件是否存在
            if not os.path.exists(pklfile):
                print(f'文件不存在: {pklfile}，跳过该股票')
                continue
            
            df_stock = pd.read_pickle(pklfile)
            if df_today_filtered is not None and not df_today_filtered.empty:  # 更新当前最新行情，否则用昨天的数据
                try:
                    # 只使用当前股票的实时行情数据
                    df_today_stock = df_today_filtered[df_today_filtered['code'] == stockcode]
                    if not df_today_stock.empty:
                        df_stock = func.update_stockquote(stockcode, df_stock, df_today_stock)
                except Exception as e:
                    print(f'更新{stockcode}实时行情失败: {e}，使用本地数据')
            
            df_stock['date'] = pd.to_datetime(df_stock['date'], format='%Y-%m-%d')  # 转为时间格式
            df_stock.set_index('date', drop=False, inplace=True)  # 时间为索引
            
            try:
                celue11_result = CeLue.macdWeeklyRangeStrategy(df_stock, start_date=start_date, end_date=end_date)
                # 检查结果是否为Series类型且不为空
                if isinstance(celue11_result, pd.Series) and len(celue11_result) > 0:
                    celue11 = celue11_result.iat[-1]
                    if celue11:
                        valid_stocklist.append(stockcode)
                else:
                    # 快速模式下可能直接返回布尔值
                    if isinstance(celue11_result, bool) and celue11_result:
                        valid_stocklist.append(stockcode)
                    else:
                        print(f'{stockcode} 周线MACD区间策略返回无效结果，跳过该股票')
            except IndexError as e:
                print(f'{stockcode} 周线MACD区间策略结果索引错误: {e}，跳过该股票')
            except AttributeError as e:
                print(f'{stockcode} 周线MACD区间策略结果属性错误: {e}，跳过该股票')
            except Exception as e:
                print(f'{stockcode} 周线MACD区间策略执行错误: {e}，跳过该股票')
        except Exception as e:
            print(f'处理{stockcode}时出错: {e}，跳过该股票')
    
    return valid_stocklist


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

# 策略函数映射，用于动态调用策略
STRATEGY_FUNCTIONS = {
    '1': 'stockSelectionStrategy',
    '2': 'buySignalStrategy',
    '3': 'highOpenLimitUpStrategy',
    '4': 'prevDayLimitUpStrategy',
    '5': 'todayLimitUpStrategy',
    '6': 'smallMarketCapStrategy',
    '7': 'turnoverRateStrategy',
    '8': 'macdWeeklyGoldenCrossStrategy',
    '9': 'macdDailyGoldenCrossStrategy',
    '10': 'continuousRiseWithNearHighStrategy',
    '11': 'macdWeeklyRangeStrategy'
}

# 主程序开始
if __name__ == '__main__':
    # 解析命令行参数
    is_single_process = 'single' in sys.argv[1:]
    
    # 获取指定的策略列表，默认为所有策略
    selected_strategies = []
    for arg in sys.argv[1:]:
        if arg in STRATEGIES:
            selected_strategies.append(arg)
    
    # 默认为执行所有策略
    if not selected_strategies:
        selected_strategies = list(STRATEGIES.keys())
    
    # 打印参数信息
    if is_single_process:
        print(f'检测到参数 single, 单进程执行')
    else:
        print(f'附带命令行参数 single 单进程执行(默认多进程)')
    
    print(f'执行策略: {[STRATEGIES[s] for s in selected_strategies]}')

    stocklist = make_stocklist()
    print(f'共 {len(stocklist)} 只候选股票')
    
    # 过滤掉没有对应pickle文件的股票代码
    print(f'过滤掉没有对应pickle文件的股票代码')
    valid_stocklist = []
    for stockcode in stocklist:
        pklfile = csvdaypath + os.sep + stockcode + '.pkl'
        if os.path.exists(pklfile):
            valid_stocklist.append(stockcode)
        else:
            print(f'文件不存在: {pklfile}，跳过该股票')
    stocklist = valid_stocklist
    print(f'过滤后剩余 {len(stocklist)} 只候选股票')

    # 由于多进程时df_dict字典占用超多内存资源，导致多进程效率还不如单进程。因此多进程模式改用函数内部读单独股票pkl文件的办法
    # print("开始载入日线文件到内存")
    # df_dict = load_dict_stock(stocklist)

    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})

    # 策略部分
    # 先判断今天是否买入
    print('今日HS300行情判断')
    df_hs300 = pd.read_csv(ucfg.tdx['csv_index'] + '/000300.csv', index_col=None, encoding='gbk', dtype={'code': str})
    df_hs300['date'] = pd.to_datetime(df_hs300['date'], format='%Y-%m-%d')  # 转为时间格式
    df_hs300.set_index('date', drop=False, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    
    # 尝试获取实时行情，添加错误处理
    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00':
        try:
            df_today = func.get_tdx_lastestquote((1, '000300'))
            df_hs300 = func.update_stockquote('000300', df_hs300, df_today)
            del df_today
        except Exception as e:
            print(f'获取实时行情失败: {e}，使用历史数据')
    HS300_信号 = CeLue.hs300SignalStrategy(df_hs300)
    if HS300_信号.iat[-1]:
        print('[red]今日HS300满足买入条件，执行买入操作[/red]')
    else:
        print('[green]今日HS300不满足买入条件，仍然选股，但不执行买入操作[/green]')
        HS300_信号.loc[:] = True  # 强制全部设置为True出选股结果


    # 周一到周五，9点到15点之间，获取在线行情。其他时间不是交易日，默认为离线数据已更新到最新
    df_today_tmppath = ucfg.tdx['csv_gbbq'] + '/df_today.pkl'
    df_today = None
    if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00' \
            and is_trading_day():
        # 获取当前最新行情，临时保存到本地，防止多次调用被服务器封IP。
        print(f'现在是交易时段，需要获取股票实时行情')
        if os.path.exists(df_today_tmppath):
            if round(time.time() - os.path.getmtime(df_today_tmppath)) < 180:  # 据创建时间小于3分钟读取本地文件
                print(f'检测到本地临时最新行情文件，读取并合并股票数据')
                df_today = pd.read_pickle(df_today_tmppath)
            else:
                # 尝试获取实时行情，添加错误处理
                try:
                    df_today = func.get_tdx_lastestquote(stocklist)
                    df_today.to_pickle(df_today_tmppath, compression=None)
                except Exception as e:
                    print(f'获取股票列表实时行情失败: {e}，使用本地数据')
                    df_today = None
        else:
            # 尝试获取实时行情，添加错误处理
            try:
                df_today = func.get_tdx_lastestquote(stocklist)
                df_today.to_pickle(df_today_tmppath, compression=None)
            except Exception as e:
                print(f'获取股票列表实时行情失败: {e}，使用本地数据')
                df_today = None
    else:
        try:
            os.remove(df_today_tmppath)
        except FileNotFoundError:
            pass
        df_today = None

    # 存储各策略的选股结果
    strategy_results = {}
    
    # 原始股票列表，用于单独执行各个策略
    original_stocklist = stocklist.copy()
    
    # 执行策略1
    if '1' in selected_strategies:
        print(f'开始执行{STRATEGIES["1"]}(mode=fast)')
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy1 = run_celue1(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue1, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue1, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy1 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy1 = stocklist_strategy1 + i.get()

        print(f'{STRATEGIES["1"]}执行完毕，已选出 {len(stocklist_strategy1):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['1'] = stocklist_strategy1
    
    # 执行策略2
    if '2' in selected_strategies:
        print(f'开始执行{STRATEGIES["2"]}')
        # 策略2的输入股票列表：如果同时执行了策略1，则使用策略1的结果，否则使用原始列表
        input_stocklist = strategy_results.get('1', original_stocklist)
        
        # 如果没有df_today
        if '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00' and df_today is None:
            try:
                df_today = func.get_tdx_lastestquote(input_stocklist)  # 获取当前最新行情
            except Exception as e:
                print(f'获取股票列表实时行情失败: {e}，使用本地数据')

        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy2 = run_celue2(input_stocklist, HS300_信号, df_gbbq, df_today)
        else:
            # 由于df_dict字典占用超多内存资源，导致多进程效率还不如单进程
            t_num = os.cpu_count() - 2  # 进程数 读取CPU逻辑处理器个数
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(input_stocklist) / t_num)
                mod = len(input_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue2, args=(input_stocklist[i * div:(i + 1) * div], HS300_信号, df_gbbq, df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue2, args=(input_stocklist[i * div:(i + 1) * div + mod], HS300_信号, df_gbbq, df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy2 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy2 = stocklist_strategy2 + i.get()

        print(f'{STRATEGIES["2"]}执行完毕，已选出 {len(stocklist_strategy2):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['2'] = stocklist_strategy2
    
    # 执行策略3：上一个交易日涨停，并且今天高开2个点以上的股票
    if '3' in selected_strategies:
        print(f'开始执行{STRATEGIES["3"]}：上一个交易日涨停，并且今天高开2个点以上的股票')
        # 策略3可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy3 = run_celue3(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue3, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue3, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy3 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy3 = stocklist_strategy3 + i.get()

        print(f'{STRATEGIES["3"]}执行完毕，已选出 {len(stocklist_strategy3):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['3'] = stocklist_strategy3
    
    # 执行策略4：上一个交易日涨停的股票，包含ST股和创业板
    if '4' in selected_strategies:
        print(f'开始执行{STRATEGIES["4"]}：上一个交易日涨停的股票，包含ST股和创业板')
        # 策略4可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy4 = run_celue4(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue4, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue4, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy4 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy4 = stocklist_strategy4 + i.get()

        print(f'{STRATEGIES["4"]}执行完毕，已选出 {len(stocklist_strategy4):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['4'] = stocklist_strategy4
    
    # 执行策略5：最近一个交易日涨停的股票，包含ST股和创业板
    if '5' in selected_strategies:
        # 判断是今日还是上一个交易日
        is_current_trading_day = is_trading_day() and '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00'
        trading_day_desc = "今日" if is_current_trading_day else "最近一个交易日"
        print(f'开始执行{STRATEGIES["5"]}：{trading_day_desc}涨停的股票，包含ST股和创业板')
        # 策略5可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy5 = run_celue5(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue5, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue5, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy5 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy5 = stocklist_strategy5 + i.get()

        print(f'{STRATEGIES["5"]}执行完毕，已选出 {len(stocklist_strategy5):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['5'] = stocklist_strategy5
    
    # 执行策略6：流通市值小于100亿的股票
    if '6' in selected_strategies:
        print(f'开始执行{STRATEGIES["6"]}：流通市值小于100亿的股票')
        # 策略6可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy6 = run_celue6(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue6, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue6, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy6 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy6 = stocklist_strategy6 + i.get()

        print(f'{STRATEGIES["6"]}执行完毕，已选出 {len(stocklist_strategy6):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['6'] = stocklist_strategy6
    
    # 执行策略7：最近一个交易日换手率大于5%且小于30%的股票
    if '7' in selected_strategies:
        # 判断是今日还是上一个交易日
        is_current_trading_day = is_trading_day() and '09:00:00' < time.strftime("%H:%M:%S", time.localtime()) < '15:00:00'
        trading_day_desc = "今日" if is_current_trading_day else "上一个交易日"
        print(f'开始执行{STRATEGIES["7"]}：{trading_day_desc}换手率大于5%且小于30%的股票')
        # 策略7可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy7 = run_celue7(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue7, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue7, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy7 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy7 = stocklist_strategy7 + i.get()

        print(f'{STRATEGIES["7"]}执行完毕，已选出 {len(stocklist_strategy7):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['7'] = stocklist_strategy7
    
    # 执行策略8：MACD周线金叉策略
    if '8' in selected_strategies:
        print(f'开始执行{STRATEGIES["8"]}：周线MACD指标出现金叉的股票')
        # 策略8可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy8 = run_celue8(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue8, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue8, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy8 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy8 = stocklist_strategy8 + i.get()

        print(f'{STRATEGIES["8"]}执行完毕，已选出 {len(stocklist_strategy8):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['8'] = stocklist_strategy8
    
    # 执行策略9：MACD日线金叉策略
    if '9' in selected_strategies:
        print(f'开始执行{STRATEGIES["9"]}：日线MACD指标出现金叉且在零轴上方的股票')
        # 策略9可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy9 = run_celue9(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue9, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue9, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy9 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy9 = stocklist_strategy9 + i.get()

        print(f'{STRATEGIES["9"]}执行完毕，已选出 {len(stocklist_strategy9):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['9'] = stocklist_strategy9
    
    # 执行策略10：持续上升且接近最高价策略
    if '10' in selected_strategies:
        print(f'开始执行{STRATEGIES["10"]}：最近三个交易日收盘价持续上升且每个交易日收盘价与最高价偏差在1个点以内的股票')
        # 策略10可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy10 = run_celue10(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue10, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue10, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy10 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy10 = stocklist_strategy10 + i.get()

        print(f'{STRATEGIES["10"]}执行完毕，已选出 {len(stocklist_strategy10):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['10'] = stocklist_strategy10
    
    # 执行策略11：周线MACD区间策略
    if '11' in selected_strategies:
        print(f'开始执行{STRATEGIES["11"]}：最近一周MACD、DIF、DEA同时大于0.05小于0.3且最近2周以上MACD值逐步放大的股票')
        # 策略11可以独立执行，使用原始股票列表
        starttime_tick = time.time()
        if is_single_process:
            stocklist_strategy11 = run_celue11(original_stocklist, df_today)
        else:
            # 进程数 读取CPU逻辑处理器个数
            if os.cpu_count() > 8:
                t_num = int(os.cpu_count() / 1.5)
            else:
                t_num = os.cpu_count() - 2
            freeze_support()  # for Windows support
            tqdm.set_lock(RLock())  # for managing output contention
            p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
            pool_result = []  # 存放pool池的返回对象列表
            for i in range(0, t_num):
                div = int(len(original_stocklist) / t_num)
                mod = len(original_stocklist) % t_num
                if i + 1 != t_num:
                    # print(i, i * div, (i + 1) * div)
                    pool_result.append(p.apply_async(run_celue11, args=(original_stocklist[i * div:(i + 1) * div], df_today, i,)))
                else:
                    # print(i, i * div, (i + 1) * div + mod)
                    pool_result.append(p.apply_async(run_celue11, args=(original_stocklist[i * div:(i + 1) * div + mod], df_today, i,)))

            # print('Waiting for all subprocesses done...')
            p.close()
            p.join()

            stocklist_strategy11 = []
            # 读取pool的返回对象列表。i.get()是读取方法。拼接每个子进程返回的df
            for i in pool_result:
                stocklist_strategy11 = stocklist_strategy11 + i.get()

        print(f'{STRATEGIES["11"]}执行完毕，已选出 {len(stocklist_strategy11):>d} 只股票 用时 {(time.time() - starttime_tick):>.2f} 秒')
        strategy_results['11'] = stocklist_strategy11
    
    # 结果
    print(f'\n全部完成 共用时 {(time.time() - starttime):>.2f} 秒')
    
    # 输出各策略的选股结果
    for strategy_id in strategy_results:
        print(f'{STRATEGIES[strategy_id]}选出 {len(strategy_results[strategy_id]):>d} 只股票:')
        # 显示股票代码和名称
        for stockcode in strategy_results[strategy_id]:
            stock_name = stock_name_dict.get(stockcode, '未知')
            print(f'  {stockcode} - {stock_name}')
    
    # 输出策略组合结果
    if len(selected_strategies) > 1:
        # 计算策略交集（同时满足所有选中策略的股票）
        all_results = [strategy_results[s] for s in selected_strategies]
        combined_result = list(set.intersection(*map(set, all_results)))
        
        print(f'\n策略组合结果（同时满足{[STRATEGIES[s] for s in selected_strategies]}）:')
        print(f'共选出 {len(combined_result):>d} 只股票:')
        # 显示股票代码和名称
        for stockcode in combined_result:
            stock_name = stock_name_dict.get(stockcode, '未知')
            print(f'  {stockcode} - {stock_name}')
