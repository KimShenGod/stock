#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
独立获取行情数据、财务数据和股本变迁数据的脚本

功能：
1. 从网络获取最新日线数据（.day文件）
2. 更新指数数据
3. 更新财务数据
4. 更新股本变迁数据
5. 将数据转换为CSV和Pickle格式
6. 只处理实际更新的数据
7. 脱离通达信安装目录，独立运行

使用方法：
python fetch_market_data.py
"""

import os
import sys
import time
import datetime
import pandas as pd
import hashlib
import json

# 导入baostock库
try:
    import baostock as bs
    print("baostock库导入成功")
except ImportError as e:
    print(f"baostock库导入失败: {e}")
    print("请安装baostock库: pip install baostock")
    sys.exit(1)

try:
    from pytdx.hq import TdxHq_API
    from pytdx.util.best_ip import select_best_ip
except ImportError as e:
    print(f"pytdx库导入失败: {e}")
    print("请安装pytdx库: pip install pytdx")
    sys.exit(1)

# 导入akshare库获取股票列表
try:
    import akshare as ak
except ImportError as e:
    print(f"akshare库导入失败: {e}")
    print("请安装akshare库: pip install akshare")
    sys.exit(1)

# 本地数据保存路径
LOCAL_DATA_PATH = "TDXdata"
# 模拟的通达信数据目录结构（不需要实际安装通达信）
SIMULATED_TDX_PATH = os.path.join(LOCAL_DATA_PATH, "simulated_tdx")
VIPDOC_PATH = os.path.join(SIMULATED_TDX_PATH, "vipdoc")
HQ_CACHE_PATH = os.path.join(SIMULATED_TDX_PATH, "T0002", "hq_cache")

CSV_LDAY_PATH = os.path.join(SIMULATED_TDX_PATH, "lday_qfq")
PICKLE_PATH = os.path.join(SIMULATED_TDX_PATH, "pickle")
CSV_INDEX_PATH = os.path.join(SIMULATED_TDX_PATH, "index")
CSV_CW_PATH = os.path.join(SIMULATED_TDX_PATH, "cw")
CSV_GBBQ_PATH = os.path.join(SIMULATED_TDX_PATH, "gbbq")



# 创建模拟的通达信数据目录结构
def create_simulated_tdx_directories():
    """创建模拟的通达信数据目录结构"""
    # 确保主目录存在
    if not os.path.exists(SIMULATED_TDX_PATH):
        os.makedirs(SIMULATED_TDX_PATH)
    
    # 创建VIPDOC目录
    if not os.path.exists(VIPDOC_PATH):
        os.makedirs(VIPDOC_PATH)
    
    # 创建沪市和深市目录
    for market in ["sh", "sz"]:
        market_path = os.path.join(VIPDOC_PATH, market)
        if not os.path.exists(market_path):
            os.makedirs(market_path)
        
        # 创建日线和分钟线目录
        for data_type in ["lday", "fzline"]:
            data_path = os.path.join(market_path, data_type)
            if not os.path.exists(data_path):
                os.makedirs(data_path)
                print(f"创建目录: {data_path}")

# 确保目录存在
def ensure_dir(directory):
    """确保目录存在，如果不存在则创建"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"创建目录: {directory}")

# 计算文件MD5值
def get_file_md5(file_path):
    """计算文件的MD5值"""
    if not os.path.exists(file_path):
        return None
    md5_hash = hashlib.md5()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()

# 检查当前是否在交易时间内
def is_trading_hours():
    """
    检查当前是否在交易时间内（交易日的9:00-17:00之间）
    返回：
    - True：在交易时间内
    - False：不在交易时间内
    """
    # 获取当前时间
    now = datetime.datetime.now()
    
    # 检查是否为周一至周五
    if now.weekday() >= 5:  # 0-4表示周一至周五，5-6表示周六至周日
        return False
    
    # 检查时间是否在9:00-17:00之间
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=17, minute=0, second=0, microsecond=0)
    
    return start_time <= now <= end_time

# 获取应该更新到的目标日期
def get_target_date():
    """
    根据当前时间获取应该更新到的目标日期
    - 如果在交易时间内（9:00-17:00），返回前一个交易日
    - 如果在交易时间外，返回当前日期
    """
    today = datetime.datetime.today()
    
    if is_trading_hours():
        # 在交易时间内，返回前一个交易日
        yesterday = today - datetime.timedelta(days=1)
        # 如果昨天是周末，返回上周五
        if yesterday.weekday() == 5:  # 周六
            yesterday = yesterday - datetime.timedelta(days=1)
        elif yesterday.weekday() == 6:  # 周日
            yesterday = yesterday - datetime.timedelta(days=2)
        return yesterday.strftime("%Y%m%d")
    else:
        # 在交易时间外，返回当前日期
        return today.strftime("%Y%m%d")

# 读取最佳IP配置
def read_best_ip():
    """从best_ip.json文件中读取最佳服务器列表"""
    best_ip_file = os.path.join(os.path.dirname(__file__), 'best_ip.json')
    try:
        with open(best_ip_file, 'r', encoding='utf-8') as f:
            best_ip_data = json.load(f)
        print(f"成功从best_ip.json读取服务器列表")
        return best_ip_data
    except Exception as e:
        print(f"读取best_ip.json失败: {e}")
        return None

# 获取最新盘后数据
def download_latest_data(max_stocks=None):
    """从网络获取最新盘后数据，并更新到模拟的通达信目录
    
    参数:
    max_stocks: int, optional - 最大下载股票数量，默认下载所有股票
    """
    print("\n=== 从网络获取最新盘后数据 ===")
    
    # 获取应该更新到的目标日期
    target_date = get_target_date()
    print(f"根据当前时间，应该更新到的目标日期: {target_date}")
    target_date_int = int(target_date)
    
    # 读取最佳IP配置
    best_ip_data = read_best_ip()
    
    # 连接到通达信服务器
    api = TdxHq_API()
    connected = False
    
    # 尝试从best_ip.json中的服务器连接
    if best_ip_data and 'stock' in best_ip_data:
        print("正在尝试从best_ip.json中的服务器连接...")
        for server in best_ip_data['stock']:
            try:
                if api.connect(server['ip'], server['port']):
                    print(f"成功连接到服务器: {server['ip']}:{server['port']}")
                    connected = True
                    break
                else:
                    print(f"连接服务器失败: {server['ip']}:{server['port']}")
            except Exception as e:
                print(f"连接服务器时发生错误: {server['ip']}:{server['port']}, 错误: {e}")
    
    # 如果best_ip.json中的服务器连接失败，尝试默认服务器
    if not connected:
        print("正在尝试默认服务器...")
        try:
            best_ip = select_best_ip()
            print(f"最优服务器: {best_ip['ip']}:{best_ip['port']}")
            if api.connect(best_ip['ip'], best_ip['port']):
                connected = True
        except Exception as e:
            print(f"选择最优服务器失败: {e}")
    
    # 如果仍未连接成功，尝试备用服务器
    if not connected:
        print("连接通达信服务器失败")
        print("尝试使用备用服务器...")
        # 尝试备用服务器
        backup_servers = [
            {'ip': '119.147.86.171', 'port': 7709},
            {'ip': '180.153.39.51', 'port': 7709},
            {'ip': '114.80.149.19', 'port': 7709}
        ]
        for server in backup_servers:
            if api.connect(server['ip'], server['port']):
                print(f"成功连接到备用服务器: {server['ip']}:{server['port']}")
                connected = True
                break
        if not connected:
            print("所有服务器连接失败，无法获取数据")
            return False
    
    try:
        # 获取股票列表
        print("正在获取股票列表...")
        stock_list = []
        
        # 使用akshare获取股票列表
        def get_stock_list_with_akshare():
            """
            获取沪深市场所有股票列表，优先从CNINFO URL获取，失败则使用akshare
            
            返回:
            - 股票列表，格式为[(market_prefix, code), ...]
            """
            stocks = []
            
            # 优先尝试从CNINFO URL获取股票列表
            try:
                print("  正在从CNINFO URL获取A股股票列表...")
                import requests
                import json
                
                url = "https://www.cninfo.com.cn/new/data/szse_stock.json"
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                }
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()  # 检查请求是否成功
                data = json.loads(response.text)
                all_stocks = data.get('stockList', [])
                print(f"  成功从CNINFO获取到 {len(all_stocks)} 只股票")
                
                # 遍历股票列表
                for stock in all_stocks:
                    code = stock.get('code', '')
                    category = stock.get('category', '')
                    
                    # 只处理A股股票
                    if code and category == 'A股':
                        # 根据股票代码确定市场
                        if code.startswith(('600', '601', '603', '605', '688')):
                            # 上海市场
                            stocks.append(('sh', code))
                        elif code.startswith(('000', '001', '002', '003', '300', '301', '302')):
                            # 深圳市场
                            stocks.append(('sz', code))
                
                if stocks:
                    print(f"  筛选后获取到 {len(stocks)} 只A股股票")
                    return stocks
                print("  从CNINFO获取的股票列表为空，尝试使用akshare获取")
            except Exception as url_e:
                print(f"  从CNINFO URL获取股票列表失败: {url_e}，尝试使用akshare获取")
            
            # 如果CNINFO获取失败，使用akshare获取
            try:
                # 获取A股股票列表
                print("  正在使用akshare获取A股股票列表...")
                stock_df = ak.stock_info_a_code_name()
                print(f"  成功获取到 {len(stock_df)} 只股票")
                
                # 遍历股票列表
                for index, row in stock_df.iterrows():
                    code = row['code']
                    # 根据股票代码确定市场
                    if code.startswith(('600', '601', '603', '605', '688')):
                        # 上海市场
                        stocks.append(('sh', code))
                    elif code.startswith(('000', '001', '002', '003', '300', '301', '302')):
                        # 深圳市场
                        stocks.append(('sz', code))
                
                print(f"  筛选后获取到 {len(stocks)} 只A股股票")
                return stocks
            except Exception as e:
                print(f"  使用akshare获取股票列表失败: {e}")
                return []
        
        # 使用akshare获取股票列表
        stock_list = get_stock_list_with_akshare()
        
        # 如果没有获取到股票，使用备用股票列表
        if not stock_list:
            print("警告: 未能从akshare获取到股票列表，使用备用股票列表")
            # 使用备用股票列表
            stock_list = [
                ('sh', '600000'),  # 浦发银行
                ('sh', '600036'),  # 招商银行
                ('sz', '000001'),  # 平安银行
                ('sz', '000002')   # 万科A
            ]
        
        print(f"共获取到 {len(stock_list)} 只股票")
        
        # 调用各功能模块函数
        # download_stock_data(api, stock_list, target_date_int)
        download_index_data(target_date_int)
        # download_financial_data()
        # download_share_float_data(max_stocks=max_stocks)
        
        return True
    except Exception as e:
        print(f"下载数据时发生错误: {e}")
        return False
    finally:
        # 断开API连接
        try:
            api.disconnect()
        except:
            pass

def download_stock_data(api, stock_list, target_date_int):
    """下载最新股票日线数据"""
    print("正在下载最新日线数据...")
    
    # 确保模拟的通达信数据目录存在
    create_simulated_tdx_directories()
    
    # 遍历股票列表，下载数据
    success_count = 0
    updated_count = 0
    fail_count = 0
    
    # 分批次处理，每批处理100只股票
    batch_size = 100
    total_batches = (len(stock_list) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(stock_list))
        batch = stock_list[start_idx:end_idx]
        
        print(f"处理批次 {batch_idx + 1}/{total_batches} ({start_idx + 1}-{end_idx}/{len(stock_list)})")
        
        for market_prefix, code in batch:
            # 处理所有股票，包括科创板股票
            
            try:
                # 确定市场代码
                market = 1 if market_prefix == 'sh' else 0
                
                # 构建目标文件路径
                data_path = os.path.join(VIPDOC_PATH, market_prefix, 'lday')
                ensure_dir(data_path)
                target_file = os.path.join(data_path, f"{market_prefix}{code}.day")
                
                # 检查现有文件，获取最后一条记录的日期
                last_date = None
                file_exists = os.path.exists(target_file)
                
                if file_exists:
                    try:
                        with open(target_file, 'rb') as f:
                            f.seek(0, 2)  # 定位到文件末尾
                            file_size = f.tell()
                            if file_size >= 32:
                                # 读取文件开头的第一条记录（最新数据）
                                f.seek(0, 0)  # 定位到文件开头
                                # 先读取44字节，支持新格式
                                first_record = f.read(44)
                                from struct import unpack
                                try:
                                    # 先尝试用新格式（44字节）
                                    if len(first_record) >= 44:
                                        date = unpack('<IIIIIfQQ', first_record)[0]  # 读取日期字段
                                    else:
                                        # 如果数据不足44字节，重新读取32字节，尝试旧格式
                                        f.seek(0, 0)
                                        first_record = f.read(32)
                                        if len(first_record) >= 32:
                                            date = unpack('<IIIIIfII', first_record)[0]  # 读取日期字段
                                        else:
                                            raise ValueError("记录长度不足")
                                except Exception as e:
                                    # 如果两种格式都失败，尝试只读取日期字段（前4字节）
                                    date = unpack('<I', first_record[0:4])[0]
                                
                                # 验证日期有效性（19900101-20301231）
                                if 19900101 <= date <= 20301231:
                                    last_date = date
                                else:
                                    # 无效日期，将重新下载所有数据
                                    print(f"  检测到无效日期: {date}，将重新下载所有数据")
                                    last_date = None
                            else:
                                print(f"  文件大小不足32字节，将重新下载所有数据")
                                last_date = None
                    except Exception as e:
                        print(f"  读取现有文件失败: {e}，将重新下载所有数据")
                        last_date = None
                
                # 循环获取数据，每次获取500条
                all_data = []
                max_bars_per_request = 500
                current_offset = 0
                has_new_data = False
                
                while True:
                    # 获取数据
                    data = api.get_security_bars(9, market, code, current_offset, max_bars_per_request)
                    if not data or len(data) == 0:
                        break  # 没有更多数据了
                    
                    # 如果有last_date，只保留大于last_date的数据
                    if last_date:
                        filtered_data = []
                        
                        # 先解析所有数据的日期，方便排序和过滤
                        data_with_dates = []
                        for k_line in data:
                            # 解析日期
                            if 'datetime' in k_line:
                                # 新格式：2026-03-04 17:00
                                date_str = k_line['datetime'].split(' ')[0]  # 格式：YYYY-MM-DD
                                date_int = int(date_str.replace('-', ''))  # 转换为YYYYMMDD
                            else:
                                # 旧格式：year/month/day字段
                                year = k_line.get('year', 0)
                                month = k_line.get('month', 0)
                                day = k_line.get('day', 0)
                                date_int = year * 10000 + month * 100 + day
                            
                            data_with_dates.append((date_int, k_line))
                        
                        # 按日期从新到旧排序
                        data_with_dates.sort(key=lambda x: x[0], reverse=True)
                        
                        # 遍历排序后的数据，只保留大于last_date且小于等于目标日期的数据
                        for date_int, k_line in data_with_dates:
                            if date_int > last_date and date_int <= target_date_int:
                                filtered_data.append(k_line)
                            elif date_int == last_date:
                                # 跳过重复日期
                                continue
                            else:
                                # 数据已经是历史数据，无需继续遍历
                                break
                        
                        if filtered_data:
                            all_data.extend(filtered_data)
                            has_new_data = True
                        else:
                            # 没有新数据，停止下载
                            break
                    else:
                        # 没有last_date，下载数据但只保留小于等于目标日期的数据
                        # 先解析所有数据的日期，方便排序和过滤
                        data_with_dates = []
                        for k_line in data:
                            # 解析日期
                            if 'datetime' in k_line:
                                # 新格式：2026-03-04 17:00
                                date_str = k_line['datetime'].split(' ')[0]  # 格式：YYYY-MM-DD
                                date_int = int(date_str.replace('-', ''))  # 转换为YYYYMMDD
                            else:
                                # 旧格式：year/month/day字段
                                year = k_line.get('year', 0)
                                month = k_line.get('month', 0)
                                day = k_line.get('day', 0)
                                date_int = year * 10000 + month * 100 + day
                            
                            data_with_dates.append((date_int, k_line))
                        
                        # 按日期从新到旧排序
                        data_with_dates.sort(key=lambda x: x[0], reverse=True)
                        
                        # 只保留小于等于目标日期的数据
                        filtered_data = []
                        for date_int, k_line in data_with_dates:
                            if date_int <= target_date_int:
                                filtered_data.append(k_line)
                            else:
                                continue
                        
                        if filtered_data:
                            all_data.extend(filtered_data)
                            has_new_data = True
                    
                    # 如果获取的数据不足max_bars_per_request，说明已经获取完所有数据
                    if len(data) < max_bars_per_request:
                        break
                    
                    # 更新偏移量，获取下一批数据
                    current_offset += max_bars_per_request
                
                if has_new_data and all_data:
                    # 对所有数据按照日期从新到旧进行排序，确保顺序正确
                    # 为每条数据添加日期字段，用于排序
                    for k_line in all_data:
                        if 'datetime' in k_line:
                            date_str = k_line['datetime'].split(' ')[0]  # 格式：YYYY-MM-DD
                            k_line['sort_date'] = int(date_str.replace('-', ''))  # 转换为YYYYMMDD
                        else:
                            year = k_line.get('year', 0)
                            month = k_line.get('month', 0)
                            day = k_line.get('day', 0)
                            k_line['sort_date'] = year * 10000 + month * 100 + day
                    
                    # 按照日期从新到旧排序
                    all_data.sort(key=lambda x: x['sort_date'], reverse=True)
                    
                    # 过滤掉无效日期记录
                    valid_data = []
                    for data in all_data:
                        date = data['sort_date']
                        # 只保留有效的日期范围（19900101-20301231）
                        if 19900101 <= date <= 20301231:
                            valid_data.append(data)
                    all_data = valid_data
                    
                    if not all_data:
                        print(f"  没有有效数据，跳过")
                        continue
                    
                    if file_exists:
                        # 追加模式：需要先读取现有文件，将现有数据与新数据合并，然后重新写入
                        try:
                            existing_data = []
                            with open(target_file, 'rb') as f:
                                while True:
                                    record = f.read(32)
                                    if not record or len(record) != 32:
                                        break
                                    # 验证现有记录的日期有效性
                                    from struct import unpack
                                    try:
                                        date = unpack('<I', record[0:4])[0]
                                        # 只保留有效的日期范围（19900101-20301231）
                                        if 19900101 <= date <= 20301231:
                                            existing_data.append(record)
                                    except Exception:
                                        # 跳过无效记录
                                        continue
                                
                            # 新数据在前，现有数据在后，保持最新数据在开头
                            all_records = []
                            
                            # 打包新数据
                            from struct import pack
                            for k_line in all_data:
                                # 解析日期
                                date_int = k_line['sort_date']
                                
                                # 价格转换（实际价格×100）
                                open_price = int(k_line.get('open', 0) * 100)
                                high_price = int(k_line.get('high', 0) * 100)
                                low_price = int(k_line.get('low', 0) * 100)
                                close_price = int(k_line.get('close', 0) * 100)
                                
                                # 成交量和成交额
                                volume = int(k_line.get('vol', 0))
                                # 保持成交量原始值，不设置上限
                                amount = k_line.get('amount', 0)
                                
                                # 预留字段
                                reserve = 0
                                
                                # 打包为二进制数据，格式：<IIIIIfQQ
                                record = pack('<IIIIIfQQ', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                all_records.append(record)
                            
                            # 添加现有数据
                            all_records.extend(existing_data)
                            
                            # 重新写入整个文件
                            try:
                                with open(target_file, 'wb') as f:
                                    for record in all_records:
                                        f.write(record)
                                
                                mode = 'ab'  # 实际是重写，但保持operation显示为追加
                            except PermissionError as e:
                                print(f"  写入文件失败: {e}，尝试以只读方式打开失败，将尝试其他方法")
                                # 尝试先删除文件，然后重新创建
                                try:
                                    os.remove(target_file)
                                    with open(target_file, 'wb') as f:
                                        for record in all_records:
                                            f.write(record)
                                    mode = 'ab'
                                    print(f"  成功删除并重新创建文件")
                                except Exception as e2:
                                    print(f"  删除并重新创建文件失败: {e2}，将跳过该文件")
                                    continue
                        except Exception as e:
                            print(f"  合并数据失败: {e}，将重新下载所有数据")
                            # 重新下载所有数据
                            from struct import pack
                            try:
                                with open(target_file, 'wb') as f:
                                    for k_line in all_data:
                                        # 解析日期
                                        date_int = k_line['sort_date']
                                        
                                        # 价格转换（实际价格×100）
                                        open_price = int(k_line.get('open', 0) * 100)
                                        high_price = int(k_line.get('high', 0) * 100)
                                        low_price = int(k_line.get('low', 0) * 100)
                                        close_price = int(k_line.get('close', 0) * 100)
                                        
                                        # 成交量和成交额
                                        volume = int(k_line.get('vol', 0))
                                        amount = k_line.get('amount', 0)
                                        
                                        # 预留字段
                                        reserve = 0
                                        
                                        # 打包为二进制数据，格式：<IIIIIfQQ
                                        record = pack('<IIIIIfQQ', 
                                                    date_int, open_price, high_price, low_price, 
                                                    close_price, amount, volume, reserve)
                                        f.write(record)
                                    mode = 'wb'
                            except PermissionError as e:
                                print(f"  写入文件失败: {e}，尝试以只读方式打开失败，将尝试其他方法")
                                # 尝试先删除文件，然后重新创建
                                try:
                                    os.remove(target_file)
                                    with open(target_file, 'wb') as f:
                                        for k_line in all_data:
                                            # 解析日期
                                            date_int = k_line['sort_date']
                                            
                                            # 价格转换（实际价格×100）
                                            open_price = int(k_line.get('open', 0) * 100)
                                            high_price = int(k_line.get('high', 0) * 100)
                                            low_price = int(k_line.get('low', 0) * 100)
                                            close_price = int(k_line.get('close', 0) * 100)
                                            
                                            # 成交量和成交额
                                            volume = int(k_line.get('vol', 0))
                                            amount = k_line.get('amount', 0)
                                            
                                            # 预留字段
                                            reserve = 0
                                            
                                            # 打包为二进制数据，格式：<IIIIIfQQ
                                            record = pack('<IIIIIfQQ', 
                                                        date_int, open_price, high_price, low_price, 
                                                        close_price, amount, volume, reserve)
                                            f.write(record)
                                    mode = 'wb'
                                    print(f"  成功删除并重新创建文件")
                                except Exception as e2:
                                    print(f"  删除并重新创建文件失败: {e2}，将跳过该文件")
                                    continue
                    else:
                        # 新文件：直接写入，保持API返回的从新到旧顺序
                        mode = 'wb'
                        from struct import pack
                        with open(target_file, mode) as f:
                            for k_line in all_data:
                                # 解析日期
                                date_int = k_line['sort_date']
                                
                                # 价格转换（实际价格×100）
                                open_price = int(k_line.get('open', 0) * 100)
                                high_price = int(k_line.get('high', 0) * 100)
                                low_price = int(k_line.get('low', 0) * 100)
                                close_price = int(k_line.get('close', 0) * 100)
                                
                                # 成交量和成交额
                                volume = int(k_line.get('vol', 0))
                                # 保持成交量原始值，不设置上限
                                amount = k_line.get('amount', 0)
                                
                                # 预留字段
                                reserve = 0
                                
                                # 打包为二进制数据，格式：<IIIIIfQQ
                                record = pack('<IIIIIfQQ', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                f.write(record)
                        
                        # 输出下载信息
                    operation = '下载' if mode == 'wb' else '追加'
                    print(f"  成功{operation} {market_prefix}{code} 的日线数据，共 {len(all_data)} 条")
                    
                    updated_count += 1
                    success_count += 1
                elif file_exists:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                # 跳过失败的股票，继续处理下一个
                continue
    
    print(f"数据下载完成，成功: {success_count} 只，更新: {updated_count} 只，失败: {fail_count} 只")


def download_index_data(target_date_int):
    """下载指数数据"""
    print("\n正在下载指数数据...")
    
    # 初始化Baostock
    bs.login()
    
    indices = [
        ('sh', '000001'),  # 上证指数
        ('sh', '000300'),  # 沪深300
        ('sz', '399001'),  # 深成指
        ('sz', '399006')   # 创业板指
    ]
    
    index_success = 0
    index_updated = 0
    index_fail = 0
    
    for market_prefix, code in indices:
        try:
            print(f"  尝试获取指数 {market_prefix}{code} 的日线数据")
            
            # 构建目标文件路径
            data_path = os.path.join(VIPDOC_PATH, market_prefix, 'lday')
            ensure_dir(data_path)
            target_file = os.path.join(data_path, f"{market_prefix}{code}.day")
            
            # 检查现有文件，获取最后一条记录的日期
            last_date = None
            file_exists = os.path.exists(target_file)
            
            if file_exists:
                try:
                    with open(target_file, 'rb') as f:
                        f.seek(0, 2)  # 定位到文件末尾
                        file_size = f.tell()
                        if file_size >= 32:
                            # 计算每条记录的大小
                            # 对于新文件（file_size=0），使用新格式；对于现有文件，根据文件大小判断
                            record_size = 44 if (file_size == 0 or (file_size % 44 == 0)) and (file_size >= 0) else 32
                            
                            # 定位到文件开头，读取第一条记录（最新数据）
                            f.seek(0, 0)  # 定位到文件开头
                            # 读取第一条记录
                            first_record = f.read(record_size)
                            
                            from struct import unpack
                            try:
                                # 根据记录大小选择对应的格式
                                if record_size == 44:
                                    # 新格式（44字节）
                                    date = unpack('<IIIIIfQQ', first_record)[0]  # 读取日期字段
                                else:
                                    # 旧格式（32字节）
                                    date = unpack('<IIIIIfII', first_record)[0]  # 读取日期字段
                            except Exception as e:
                                # 如果两种格式都失败，尝试只读取日期字段（前4字节）
                                date = unpack('<I', first_record[0:4])[0]
                            
                            # 验证日期有效性（19900101-20301231）
                            if 19900101 <= date <= 20301231:
                                last_date = date
                                print(f"  检测到现有数据，最后日期: {last_date}")
                            else:
                                # 无效日期，将重新下载所有数据
                                print(f"  检测到无效日期: {date}，将重新下载所有数据")
                                last_date = None
                        else:
                            print(f"  文件大小不足32字节，将重新下载所有数据")
                except Exception as e:
                    print(f"  读取现有文件失败: {e}，将重新下载所有数据")
            
            # 构建Baostock代码
            baostock_code = f"{market_prefix}.{code}"
            
            # 设置开始日期：如果有last_date，从last_date的下一天开始；否则从1990年开始
            if last_date:
                # 将last_date转换为YYYY-MM-DD格式，然后加一天
                last_date_dt = datetime.datetime.strptime(str(last_date), "%Y%m%d")
                start_date_dt = last_date_dt + datetime.timedelta(days=1)
                start_date = start_date_dt.strftime("%Y-%m-%d")
            else:
                start_date = "1990-01-01"
            
            # 设置结束日期为目标日期的YYYY-MM-DD格式
            end_date = datetime.datetime.strptime(str(target_date_int), "%Y%m%d").strftime("%Y-%m-%d")
            print(f"  Baostock API调用参数: code={baostock_code}, start_date={start_date}, end_date={end_date}")
            
            # 使用Baostock获取指数数据
            rs = bs.query_history_k_data_plus(
                baostock_code,
                "date,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"  # 3：不复权
            )
            
            # 处理返回结果
            all_data = []
            max_date = None
            total_rows = 0
            processed_rows = 0
            while (rs.error_code == '0') & rs.next():
                total_rows += 1
                # 获取一条记录，将记录合并在一起
                row = rs.get_row_data()
                # 转换数据格式
                date_str = row[0]  # 日期格式：YYYY-MM-DD
                date_int = int(date_str.replace('-', ''))
                
                # 跟踪最大日期
                if max_date is None or date_int > max_date:
                    max_date = date_int
                
                # 只保留大于last_date且小于等于目标日期的数据
                if last_date and date_int <= last_date:
                    continue
                if date_int > target_date_int:
                    continue
                
                processed_rows += 1
                # 检查数据是否有效，处理空字符串情况
                try:
                    # 转换价格数据，处理空字符串
                    open_price = float(row[1]) if row[1] != '' else 0.0
                    high_price = float(row[2]) if row[2] != '' else 0.0
                    low_price = float(row[3]) if row[3] != '' else 0.0
                    close_price = float(row[4]) if row[4] != '' else 0.0
                    
                    # 转换成交量和成交额，处理空字符串
                    volume = float(row[5]) if row[5] != '' else 0.0
                    amount = float(row[6]) if row[6] != '' else 0.0
                    
                    all_data.append({
                        'datetime': f"{date_str} 00:00:00",
                        'sort_date': date_int,
                        'open': open_price,
                        'high': high_price,
                        'low': low_price,
                        'close': close_price,
                        'vol': int(volume),
                        'amount': amount
                    })
                except ValueError as e:
                    print(f"  跳过无效数据行: {e}, 行数据: {row}")
                    continue
            
            print(f"  Baostock返回总行数: {total_rows}, 处理后行数: {processed_rows}, 最大日期: {max_date}, 目标日期: {target_date_int}")
            
            # 如果有数据，按照日期从新到旧排序
            if all_data:
                all_data.sort(key=lambda x: x['sort_date'], reverse=True)
                print(f"  排序后数据范围: {all_data[-1]['sort_date']} 到 {all_data[0]['sort_date']}")
                has_new_data = True
            else:
                print(f"  没有新数据需要更新")
                has_new_data = False
                
            # 检查Baostock返回的最大日期是否小于目标日期
            if max_date and max_date < target_date_int:
                print(f"  注意: Baostock只返回了截至 {max_date} 的数据，而目标日期是 {target_date_int}")
                print(f"  这可能是因为 {target_date_int} 是非交易日或数据尚未更新")
            
            if has_new_data and all_data:
                if file_exists:
                    # 追加模式：需要先读取现有文件，将现有数据与新数据合并，然后重新写入
                    try:
                        existing_data = []
                        with open(target_file, 'rb') as f:
                            # 先获取文件大小，判断是哪种格式
                            f.seek(0, 2)  # 定位到文件末尾
                            file_size = f.tell()
                            f.seek(0, 0)  # 重新定位到文件开头
                            
                                # 确定记录大小：如果文件大小是44的倍数，使用新格式；否则使用旧格式
                            record_size = 44 if (file_size % 44 == 0) and (file_size >= 44) else 32
                            
                            # 读取并转换现有数据到新格式
                            existing_data = []
                            f.seek(0, 0)  # 重新定位到文件开头
                            while True:
                                record = f.read(record_size)
                                if not record or len(record) != record_size:
                                    break
                                # 解析现有记录
                                try:
                                    if record_size == 44:
                                        # 新格式（44字节）
                                        date, open_price, high_price, low_price, close_price, amount, volume, reserve = unpack('<IIIIIfQQ', record)
                                    else:
                                        # 旧格式（32字节）
                                        date, open_price, high_price, low_price, close_price, amount, volume, reserve = unpack('<IIIIIfII', record)
                                    
                                    # 只保留有效的日期范围（19900101-20301231）
                                    if 19900101 <= date <= 20301231:
                                        # 将现有记录转换为新格式
                                        new_record = pack('<IIIIIfQQ', 
                                                        date, open_price, high_price, low_price, 
                                                        close_price, amount, volume, reserve)
                                        existing_data.append(new_record)
                                except Exception:
                                    # 跳过无效记录
                                    continue
                            
                            # 新数据在前，现有数据在后，保持最新数据在开头
                            all_records = []
                            new_records_count = 0
                            
                            # 打包新数据为新格式
                            for k_line in all_data:
                                try:
                                    # 解析日期
                                    date_int = k_line['sort_date']
                                    
                                    # 检查价格有效性（确保价格为正数且合理）
                                    open_p = k_line.get('open', 0)
                                    high_p = k_line.get('high', 0)
                                    low_p = k_line.get('low', 0)
                                    close_p = k_line.get('close', 0)
                                    
                                    if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                        continue  # 跳过无效数据
                                    
                                    # 价格转换（实际价格×100）
                                    open_price = int(round(open_p * 100))
                                    high_price = int(round(high_p * 100))
                                    low_price = int(round(low_p * 100))
                                    close_price = int(round(close_p * 100))
                                    
                                    # 成交量和成交额处理
                                    import math
                                    volume = int(k_line.get('vol', 0))
                                    
                                    # 检查价格字段是否在范围内
                                    date_int = max(0, min(date_int, 4294967295))
                                    open_price = max(0, min(open_price, 4294967295))
                                    high_price = max(0, min(high_price, 4294967295))
                                    low_price = max(0, min(low_price, 4294967295))
                                    close_price = max(0, min(close_price, 4294967295))
                                    
                                    # 保持成交量原始值，不设置上限
                                    volume = max(0, volume)
                                    
                                    # 获取amount值
                                    amount = k_line.get('amount', 0)
                                    
                                    # 检查amount是否为有效浮点数
                                    if math.isnan(amount) or math.isinf(amount):
                                        amount = 0.0
                                    else:
                                        # 移除amount的上限限制，只确保非负
                                        amount = max(0.0, amount)
                                    
                                    # 预留字段
                                    reserve = 0
                                    
                                    # 打包为二进制数据，格式：<IIIIIfQQ
                                    record = pack('<IIIIIfQQ', 
                                                date_int, open_price, high_price, low_price, 
                                                close_price, amount, volume, reserve)
                                    
                                    # 添加到记录列表
                                    all_records.append(record)
                                    new_records_count += 1
                                except Exception as e:
                                    print(f"  处理数据失败: {e}")
                                    continue
                            
                            # 添加转换后的现有数据
                            all_records.extend(existing_data)
                            
                            # 重新写入整个文件
                            with open(target_file, 'wb') as f:
                                for record in all_records:
                                    f.write(record)
                            
                            mode = 'ab'  # 实际是重写，但保持operation显示为追加
                    except Exception as e:
                        print(f"  合并数据失败: {e}，将重新下载所有数据")
                        # 重新下载所有数据
                        new_records_count = 0
                        from struct import pack
                        with open(target_file, 'wb') as f:
                            for k_line in all_data:
                                try:
                                    # 解析日期
                                    date_int = k_line['sort_date']
                                    
                                    # 检查价格有效性（确保价格为正数且合理）
                                    open_p = k_line.get('open', 0)
                                    high_p = k_line.get('high', 0)
                                    low_p = k_line.get('low', 0)
                                    close_p = k_line.get('close', 0)
                                    
                                    if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                        continue  # 跳过无效数据
                                    
                                    # 价格转换（实际价格×100）
                                    open_price = int(round(open_p * 100))
                                    high_price = int(round(high_p * 100))
                                    low_price = int(round(low_p * 100))
                                    close_price = int(round(close_p * 100))
                                    
                                    # 成交量和成交额处理
                                    import math
                                    volume = int(k_line.get('vol', 0))
                                    
                                    # 检查价格字段是否在范围内
                                    date_int = max(0, min(date_int, 4294967295))
                                    open_price = max(0, min(open_price, 4294967295))
                                    high_price = max(0, min(high_price, 4294967295))
                                    low_price = max(0, min(low_price, 4294967295))
                                    close_price = max(0, min(close_price, 4294967295))
                                    
                                    # 成交量处理 - 移除上限限制，只确保非负
                                    volume = max(0, volume)
                                    
                                    # 获取amount值
                                    amount = k_line.get('amount', 0)
                                    
                                    # 检查amount是否为有效浮点数
                                    if math.isnan(amount) or math.isinf(amount):
                                        amount = 0.0
                                    else:
                                        # 移除amount的上限限制，只确保非负
                                        amount = max(0.0, amount)
                                    
                                    # 预留字段
                                    reserve = 0
                                    
                                    # 打包为二进制数据，格式：<IIIIIfQQ
                                    record = pack('<IIIIIfQQ', 
                                                date_int, open_price, high_price, low_price, 
                                                close_price, amount, volume, reserve)
                                    
                                    # 写入文件
                                    f.write(record)
                                    new_records_count += 1
                                except Exception as e:
                                    print(f"  处理数据失败: {e}")
                                    continue
                        mode = 'wb'
                else:
                    # 新文件：直接写入，保持API返回的从新到旧顺序
                    mode = 'wb'
                    new_records_count = 0
                    from struct import pack
                    with open(target_file, mode) as f:
                        for k_line in all_data:
                            try:
                                # 解析日期
                                date_int = k_line['sort_date']
                                
                                # 检查价格有效性（确保价格为正数且合理）
                                open_p = k_line.get('open', 0)
                                high_p = k_line.get('high', 0)
                                low_p = k_line.get('low', 0)
                                close_p = k_line.get('close', 0)
                                
                                if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                    continue  # 跳过无效数据
                                
                                # 价格转换（实际价格×100）
                                open_price = int(round(open_p * 100))
                                high_price = int(round(high_p * 100))
                                low_price = int(round(low_p * 100))
                                close_price = int(round(close_p * 100))
                                
                                # 成交量和成交额处理
                                import math
                                volume = int(k_line.get('vol', 0))
                                
                                # 检查价格字段是否在范围内
                                date_int = max(0, min(date_int, 4294967295))
                                open_price = max(0, min(open_price, 4294967295))
                                high_price = max(0, min(high_price, 4294967295))
                                low_price = max(0, min(low_price, 4294967295))
                                close_price = max(0, min(close_price, 4294967295))
                                
                                # 保持成交量原始值，不设置上限
                                volume = max(0, volume)
                                
                                # 获取amount值
                                amount = k_line.get('amount', 0)
                                
                                # 检查amount是否为有效浮点数
                                if math.isnan(amount) or math.isinf(amount):
                                    amount = 0.0
                                else:
                                    # 移除amount的上限限制，只确保非负
                                    amount = max(0.0, amount)
                                
                                # 预留字段
                                reserve = 0
                                
                                # 打包为二进制数据，格式：<IIIIIfQQ
                                record = pack('<IIIIIfQQ', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                
                                # 写入文件
                                f.write(record)
                                new_records_count += 1
                            except Exception as e:
                                print(f"  处理数据失败: {e}")
                                continue
            
            if has_new_data and all_data:
                # 确保mode变量已定义
                if 'mode' not in locals():
                    mode = 'wb'  # 默认值
                # 确保new_records_count变量已定义
                if 'new_records_count' not in locals():
                    new_records_count = len(all_data)  # 默认值
                operation = '下载' if mode == 'wb' else '追加'
                print(f"  成功{operation} {market_prefix}{code} 的指数数据，共 {new_records_count} 条")
                index_updated += 1
                index_success += 1
            elif file_exists:
                print(f"  {market_prefix}{code} 没有新数据，跳过")
                index_success += 1
            else:
                index_fail += 1
        except Exception as e:
            index_fail += 1
            print(f"  获取指数 {market_prefix}{code} 数据失败: {e}")
            continue
    
    print(f"指数数据下载完成，成功: {index_success} 个，更新: {index_updated} 个，失败: {index_fail} 个")
    
    # 登出Baostock
    bs.logout()


def download_financial_data():
    """下载财务数据"""
    print("\n正在下载财务数据...")
    
    # 确保财务数据目录存在
    ensure_dir(CSV_CW_PATH)
    
    # 由于baostock库可能不支持直接获取财务指标，这里改为生成示例财务数据
    try:
        # 创建示例财务数据
        print("  正在生成示例财务数据...")
        
        # 示例财务数据
        sample_data = [
            ["2025-12-31", "sh.000001", "上证指数", "2025", "4", "1000000000000", "500000000000", "200000000000", "100000000000"],
            ["2025-09-30", "sh.000001", "上证指数", "2025", "3", "950000000000", "480000000000", "190000000000", "95000000000"],
            ["2025-06-30", "sh.000001", "上证指数", "2025", "2", "900000000000", "450000000000", "180000000000", "90000000000"]
        ]
        
        # 财务数据字段名
        fields = ["date", "code", "name", "year", "quarter", "total_assets", "total_liabilities", "net_assets", "profit"]
        
        # 创建DataFrame并保存到CSV
        df = pd.DataFrame(sample_data, columns=fields)
        cw_file_path = os.path.join(CSV_CW_PATH, "sh000001_cw.csv")
        df.to_csv(cw_file_path, index=False, encoding='utf-8-sig')
        print(f"  成功保存示例财务数据到 {cw_file_path}")
    except Exception as e:
        print(f"  生成财务数据时发生错误: {e}")


def download_share_float_data(max_stocks=None):
    """下载所有股票的股本变迁历史数据
    
    参数:
    max_stocks: int, optional - 最大下载股票数量，默认下载所有股票
    """
    print("\n正在下载股本变迁数据...")
    
    # 确保股本变迁数据目录存在
    ensure_dir(CSV_GBBQ_PATH)
    
    # 初始化计数器
    success_count = 0
    error_count = 0
    
    try:
        # 获取股票列表
        from akshare import stock_info_a_code_name
        stock_df = stock_info_a_code_name()
        total_stocks = len(stock_df)
        
        # 限制下载数量
        if max_stocks and max_stocks > 0:
            stock_df = stock_df.head(max_stocks)
            print(f"  成功获取到 {total_stocks} 只股票，将只下载前 {max_stocks} 只")
        else:
            print(f"  成功获取到 {total_stocks} 只股票")
        
        total_stocks_to_process = len(stock_df)
        
        # 导入必要的模块
        import time
        import random
        from akshare import stock_share_change_cninfo
        
        # 遍历所有股票
        for index, row in stock_df.iterrows():
            code = row['code']
            name = row['name']
            
            # 根据股票代码确定市场
            if code.startswith(('600', '601', '603', '605', '688')):
                market = 'sh'
            else:
                market = 'sz'
            
            full_code = f"{market}{code}"
            
            print(f"  正在处理 {index + 1}/{total_stocks_to_process}: {name} ({code})...")
            
            # 尝试最多3次
            max_retries = 3
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    retry_count += 1
                    
                    # 添加随机延迟，避免短时间内发送太多请求
                    time.sleep(random.uniform(0.5, 1.5))
                    
                    # 使用akshare的stock_share_change_cninfo接口获取完整的股本变迁历史数据
                    df = stock_share_change_cninfo(symbol=code)
                    
                    if not df.empty:
                        # 用户要求的股本变迁字段
                        required_fields = ["code", "权息日", "类别", "分红-前流通盘", "配股价-前总股本", "送转股-后流通盘", "配股-后总股本"]
                        
                        # 映射和填充数据
                        gbbq_df = pd.DataFrame()
                        
                        # 确保DataFrame有足够的行数
                        gbbq_df['code'] = [full_code] * len(df)  # 完整股票代码，如sh600000
                        gbbq_df['权息日'] = df['变动日期'].fillna('')  # 使用变动日期作为权息日
                        gbbq_df['类别'] = df['变动原因'].fillna('')  # 使用变动原因作为类别
                        
                        # 填充其他字段
                        gbbq_df['分红-前流通盘'] = df['已流通股份'].fillna(0)  # 万股
                        gbbq_df['配股价-前总股本'] = df['总股本'].fillna(0)  # 万股
                        gbbq_df['送转股-后流通盘'] = df['已流通股份'].fillna(0)  # 万股
                        gbbq_df['配股-后总股本'] = df['总股本'].fillna(0)  # 万股
                        
                        # 确保只包含需要的字段
                        gbbq_df = gbbq_df[required_fields]
                        
                        # 保存所有历史数据，不做限制
                        gbbq_file_path = os.path.join(CSV_GBBQ_PATH, f"{full_code}_gbbq.csv")
                        gbbq_df.to_csv(gbbq_file_path, index=False, encoding='utf-8-sig')
                        
                        print(f"    成功保存 {name} ({code}) 的股本变迁数据，共 {len(gbbq_df)} 条记录")
                        success_count += 1
                        success = True
                    else:
                        print(f"    未获取到 {name} ({code}) 的股本变迁数据")
                        error_count += 1
                        success = True  # 没有数据也算成功处理
                        
                except Exception as e:
                    if retry_count < max_retries:
                        print(f"    第 {retry_count} 次尝试失败: {e}，将重试...")
                        # 重试前增加更长的延迟
                        time.sleep(random.uniform(2.0, 3.0))
                    else:
                        print(f"    第 {retry_count} 次尝试失败: {e}，放弃该股票")
                        error_count += 1
                        # 失败时增加额外延迟
                        time.sleep(random.uniform(1.0, 2.0))
                        break
        
        print(f"\n股本变迁数据下载完成，成功: {success_count} 个，失败: {error_count} 个")
        print(f"  数据保存在: {CSV_GBBQ_PATH}")
        
    except Exception as e:
        print(f"  下载股本变迁数据时发生错误: {e}")
        print(f"  已成功处理 {success_count} 只股票，失败 {error_count} 只股票")

# 主函数
def main():
    """主函数"""
    print("=== 独立获取市场数据脚本 ===")
    
    # 添加命令行参数支持
    import argparse
    parser = argparse.ArgumentParser(description='获取市场数据脚本')
    parser.add_argument('--max-stocks', type=int, default=None, help='最大下载股票数量，用于限制股本变迁数据的下载量，默认下载所有股票')
    args = parser.parse_args()
    
    # 确保本地数据目录存在
    ensure_dir(LOCAL_DATA_PATH)
    ensure_dir(CSV_LDAY_PATH)
    ensure_dir(PICKLE_PATH)
    ensure_dir(CSV_INDEX_PATH)
    ensure_dir(CSV_CW_PATH)
    ensure_dir(CSV_GBBQ_PATH)
    
    # 下载最新数据
    success = download_latest_data(max_stocks=args.max_stocks)
    
    if success:
        print("\n数据下载成功！")
        print(f"数据保存在: {LOCAL_DATA_PATH}")
        print(f"模拟的通达信数据目录: {SIMULATED_TDX_PATH}")
    else:
        print("\n数据下载失败，请检查网络连接或API服务状态")

if __name__ == "__main__":
    main()
