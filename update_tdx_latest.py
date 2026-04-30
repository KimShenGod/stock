#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
更新东方财富通达信盘后数据脚本

功能：
1. 从网络获取最新日线数据（.day文件）
2. 更新指数数据
3. 更新财务数据
4. 更新股本变迁数据
5. 将数据转换为CSV和Pickle格式
6. 只处理实际更新的数据

使用方法：
python update_tdx_latest.py
"""

import os
import sys
import time
import datetime
import pandas as pd
import shutil
import hashlib
import user_config as ucfg
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

# 配置通达信安装路径
TDX_PATH = ucfg.tdx['tdx_path']
VIPDOC_PATH = os.path.join(TDX_PATH, "vipdoc")
HQ_CACHE_PATH = os.path.join(TDX_PATH, "T0002", "hq_cache")

# 本地数据保存路径
LOCAL_DATA_PATH = "TDXdata"
CSV_LDAY_PATH = ucfg.tdx['csv_lday']
PICKLE_PATH = ucfg.tdx['pickle']
CSV_INDEX_PATH = ucfg.tdx['csv_index']
CSV_CW_PATH = ucfg.tdx['csv_cw']
CSV_GBBQ_PATH = ucfg.tdx['csv_gbbq']

# 创建通达信数据目录结构
def create_tdx_directories():
    """创建通达信数据目录结构"""
    # 确保主目录存在
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
def download_latest_data():
    """从网络获取最新盘后数据，并更新到通达信安装目录"""
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
        
        # 下载最新日线数据
        print("正在下载最新日线数据...")
        
        # 确保通达信数据目录存在
        
        # 遍历股票列表，下载数据
        success_count = 0
        updated_count = 0
        fail_count = 0
        
        # 分批次处理，每批处理100只股票
        batch_size = 100
        total_batches = (len(stock_list) + batch_size - 1) // batch_size
        
        # 快速检测：检查前2个批次的day文件最新日期是否都等于目标日期
        quick_check_batches = 2
        quick_check_passed = False
        
        # 统计已存在的day文件数量
        existing_file_count = 0
        for market_prefix, code in stock_list[:quick_check_batches * batch_size]:
            if code.startswith('688'):
                continue
            target_file = os.path.join(VIPDOC_PATH, market_prefix, 'lday', f"{market_prefix}{code}.day")
            if os.path.exists(target_file):
                existing_file_count += 1
        
        if existing_file_count > 0 and total_batches >= quick_check_batches:
            print(f"\n=== 开始快速检测 ===")
            print(f"前{quick_check_batches}个批次中共有{existing_file_count}只股票有已存在的day文件")
            all_up_to_date = True
            checked_count = 0
            
            for check_batch_idx in range(quick_check_batches):
                start_idx = check_batch_idx * batch_size
                end_idx = min((check_batch_idx + 1) * batch_size, len(stock_list))
                batch = stock_list[start_idx:end_idx]
                
                for market_prefix, code in batch:
                    if code.startswith('688'):
                        continue
                    
                    target_file = os.path.join(VIPDOC_PATH, market_prefix, 'lday', f"{market_prefix}{code}.day")
                    if os.path.exists(target_file):
                        try:
                            with open(target_file, 'rb') as f:
                                f.seek(0, 2)
                                file_size = f.tell()
                                if file_size >= 32:
                                    f.seek(-32, 2)
                                    last_record = f.read(32)
                                    from struct import unpack
                                    date = unpack('<IIIIIfII', last_record)[0]
                                    if 19900101 <= date <= 20301231:
                                        if date != target_date_int:
                                            print(f"  发现{market_prefix}{code}的最新日期为{date}，目标日期为{target_date_int}")
                                            all_up_to_date = False
                                            break
                                    checked_count += 1
                        except Exception as e:
                            print(f"  读取{market_prefix}{code}的day文件失败: {e}")
                if not all_up_to_date:
                    break
            
            if all_up_to_date and checked_count > 0:
                print(f"快速检测完成：前{quick_check_batches}个批次({checked_count}只股票)的day文件最新日期均为{target_date_int}")
                print(f"判定所有股票日线数据已是最新，跳过个股日线更新")
                quick_check_passed = True
            else:
                if checked_count == 0:
                    print(f"快速检测完成：前{quick_check_batches}个批次中没有找到可读取的day文件，继续处理所有批次")
                else:
                    print(f"快速检测完成：发现{checked_count}只股票中有日期不等于{target_date_int}的，继续处理所有批次")
        else:
            print(f"\n=== 快速检测跳过 ===")
            print(f"前{quick_check_batches}个批次中只有{existing_file_count}只股票有已存在的day文件，跳过快速检测")
        
        if not quick_check_passed:
            for batch_idx in range(total_batches):
                start_idx = batch_idx * batch_size
                end_idx = min((batch_idx + 1) * batch_size, len(stock_list))
                batch = stock_list[start_idx:end_idx]
                
                print(f"处理批次 {batch_idx + 1}/{total_batches} ({start_idx + 1}-{end_idx}/{len(stock_list)})")
                
                for market_prefix, code in batch:
                    # 只处理A股股票，跳过其他类型
                    # 沪市A股：600、601、603、605开头
                    # 深市A股：000、001、002、300、301、302开头
                    # 跳过：科创板(688)、债券、基金等
                    if market_prefix == 'sh':
                        # 沪市A股
                        if not (code.startswith('600') or code.startswith('601') or code.startswith('603') or code.startswith('605')):
                            continue
                    else:
                        # 深市A股
                        if not (code.startswith('000') or code.startswith('001') or code.startswith('002') or code.startswith('300') or code.startswith('301') or code.startswith('302')):
                            continue
                        
                    try:
                        # 确定市场代码
                        market = 1 if market_prefix == 'sh' else 0
                        
                        # 构建目标文件路径
                        data_path = os.path.join(VIPDOC_PATH, market_prefix, 'lday')
                        ensure_dir(data_path)
                        target_file = os.path.join(data_path, f"{market_prefix}{code}.day")
                        
                        # 检查现有文件，获取最后一条记录的日期（最新日期在文件末尾）
                        last_date = None
                        file_exists = os.path.exists(target_file)
                        
                        if file_exists:
                            try:
                                with open(target_file, 'rb') as f:
                                    f.seek(0, 2)  # 定位到文件末尾
                                    file_size = f.tell()
                                    if file_size >= 32:
                                        # 读取文件末尾的最后一条记录（最新数据）
                                        f.seek(-32, 2)  # 定位到文件末尾前32字节
                                        last_record = f.read(32)
                                        from struct import unpack
                                        date = unpack('<IIIIIfII', last_record)[0]  # 读取日期字段
                                        
                                        # 验证日期有效性（19900101-20301231）
                                        if 19900101 <= date <= 20301231:
                                            last_date = date
                                            # print(f"  检测到现有文件，最后日期: {last_date}")
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
                            # 对所有数据按照日期从旧到新进行排序，确保顺序正确
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
                            
                            # 按照日期从旧到新排序
                            all_data.sort(key=lambda x: x['sort_date'])
                            
                            # 过滤掉无效日期记录
                            valid_data = []
                            for data in all_data:
                                date = data['sort_date']
                                # 只保留有效的日期范围（19900101-20301231）
                                if 19900101 <= date <= 20301231:
                                    valid_data.append(data)
                            all_data = valid_data
                            
                            if not all_data:
                                # 如果没有新数据且文件已存在，直接跳过（保留原文件）
                                if file_exists:
                                    print(f"  {market_prefix}{code} 没有新数据，跳过")
                                    success_count += 1
                                else:
                                    print(f"  {market_prefix}{code} 没有有效数据，跳过")
                                continue
                            
                            # 初始化现有数据列表
                            existing_data = []
                            
                            if file_exists:
                                # 追加模式：需要先读取现有文件，将现有数据与新数据合并，然后重新写入
                                try:
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
                                except Exception as e:
                                    print(f"  读取现有文件失败: {e}，将重新下载所有数据")
                            
                            # 现有数据在前，新数据在后，保持最新数据在末尾
                            all_records = []
                            
                            # 添加现有数据
                            all_records.extend(existing_data)
                            
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
                                amount = k_line.get('amount', 0)
                                
                                # 预留字段
                                reserve = 0
                                
                                # 打包为二进制数据，格式：<IIIIIfII
                                record = pack('<IIIIIfII', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                all_records.append(record)
                            
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
                                            
                                            # 打包为二进制数据，格式：<IIIIIfII
                                            record = pack('<IIIIIfII', 
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
                                                
                                                # 打包为二进制数据，格式：<IIIIIfII
                                                record = pack('<IIIIIfII', 
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
                                    amount = k_line.get('amount', 0)
                                    
                                    # 预留字段
                                    reserve = 0
                                    
                                    # 打包为二进制数据，格式：<IIIIIfII
                                    record = pack('<IIIIIfII', 
                                                date_int, open_price, high_price, low_price, 
                                                close_price, amount, volume, reserve)
                                    f.write(record)
                        
                        # 输出下载信息
                        operation = '下载' if mode == 'wb' else '追加'
                        print(f"  成功{operation} {market_prefix}{code} 的日线数据，共 {len(all_data)} 条")
                        
                        updated_count += 1
                        success_count += 1
                    except Exception as e:
                        fail_count += 1
                        # 打印失败信息
                        print(f"  处理 {market_prefix}{code} 失败: {str(e)}")
                        # 跳过失败的股票，继续处理下一个
                        continue
        
        print(f"数据下载完成，成功: {success_count} 只，更新: {updated_count} 只，失败: {fail_count} 只")
        
        # 删除0KB的day文件
        print("\n清理0KB的day文件...")
        zero_kb_count = 0
        for market_prefix in ['sh', 'sz']:
            lday_path = os.path.join(VIPDOC_PATH, market_prefix, 'lday')
            if os.path.exists(lday_path):
                for file in os.listdir(lday_path):
                    if file.endswith('.day'):
                        file_path = os.path.join(lday_path, file)
                        try:
                            if os.path.getsize(file_path) == 0:
                                os.remove(file_path)
                                zero_kb_count += 1
                        except Exception as e:
                            print(f"  删除 {file_path} 失败: {e}")
        if zero_kb_count > 0:
            print(f"  已删除 {zero_kb_count} 个0KB的day文件")
        else:
            print("  没有发现0KB的day文件")
        
        # 下载指数数据
        print("\n正在下载指数数据...")
        
        # 使用Baostock获取指数数据
        print("  使用Baostock获取指数数据")
        bs.login_result = bs.login()
        if bs.login_result.error_code != '0':
            print(f"  Baostock登录失败: {bs.login_result.error_msg}")
            print("  无法获取指数数据，请检查网络连接或稍后重试")
            return
        else:
            print("  Baostock登录成功")
        
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
                
                # 指数数据采用全量下载模式
                file_exists = os.path.exists(target_file)
                if file_exists:
                    print(f"  检测到现有文件，将进行全量更新覆盖")
                else:
                    print(f"  未检测到现有文件，将下载全部历史数据")
                
                # 设置开始日期
                start_date = "1990-01-01"
                
                # 设置结束日期为目标日期的YYYY-MM-DD格式
                end_date = datetime.datetime.strptime(str(target_date_int), "%Y%m%d").strftime("%Y-%m-%d")
                
                # 处理返回结果
                all_data = []
                max_date = None
                total_rows = 0
                processed_rows = 0
                
                # 使用Baostock获取指数数据（优先）
                baostock_code = f"{market_prefix}.{code}"
                print(f"  使用Baostock获取指数 {market_prefix}{code} 的数据")
                print(f"  Baostock API调用参数: code={baostock_code}, start_date={start_date}, end_date={end_date}")
                
                rs = bs.query_history_k_data_plus(
                    baostock_code,
                    "date,open,high,low,close,volume,amount",
                    start_date=start_date,
                    end_date=end_date,
                    frequency="d",
                    adjustflag="3"  # 3：不复权
                )
                
                while (rs.error_code == '0') & rs.next():
                    total_rows += 1
                    row = rs.get_row_data()
                    date_str = row[0]
                    date_int = int(date_str.replace('-', ''))
                    
                    if max_date is None or date_int > max_date:
                        max_date = date_int
                    
                    if date_int > target_date_int:
                        continue
                    
                    processed_rows += 1
                    try:
                        open_price = float(row[1]) if row[1] != '' else 0.0
                        high_price = float(row[2]) if row[2] != '' else 0.0
                        low_price = float(row[3]) if row[3] != '' else 0.0
                        close_price = float(row[4]) if row[4] != '' else 0.0
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
                    except Exception as e:
                        print(f"  跳过无效数据行: {e}, 行数据: {row}")
                        continue

                print(f"  Baostock返回总行数: {total_rows}, 处理后行数: {processed_rows}, 最大日期: {max_date}, 目标日期: {target_date_int}")
                
                # 如果有数据，按照日期从旧到新排序
                if all_data:
                    all_data.sort(key=lambda x: x['sort_date'])
                    print(f"  排序后数据范围: {all_data[0]['sort_date']} 到 {all_data[-1]['sort_date']}")
                    has_new_data = True
                else:
                    print(f"  没有新数据需要更新")
                    has_new_data = False
                    
                # 检查Baostock返回的最大日期是否小于目标日期
                if max_date and max_date < target_date_int:
                    print(f"  注意: Baostock只返回了截至 {max_date} 的数据，而目标日期是 {target_date_int}")
                    print(f"  这可能是因为 {target_date_int} 是非交易日或数据尚未更新")
                
                if has_new_data and all_data:
                    # 指数数据采用全量覆盖模式
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
                                
                                # 检查所有整数字段是否在范围内
                                date_int = max(0, min(date_int, 4294967295))
                                open_price = max(0, min(open_price, 4294967295))
                                high_price = max(0, min(high_price, 4294967295))
                                low_price = max(0, min(low_price, 4294967295))
                                close_price = max(0, min(close_price, 4294967295))
                                volume = max(0, min(volume, 4294967295))
                                
                                # 获取amount值
                                amount = k_line.get('amount', 0)
                                
                                # 检查amount是否为有效浮点数
                                if math.isnan(amount) or math.isinf(amount):
                                    amount = 0.0
                                else:
                                    # 限制amount在float可表示的合理范围内
                                    amount = max(-1e38, min(amount, 1e38))
                                
                                # 预留字段
                                reserve = 0
                                
                                # 打包为二进制数据，格式：<IIIIIfII
                                record = pack('<IIIIIfII', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                
                                # 写入文件
                                f.write(record)
                                new_records_count += 1
                            except Exception as e:
                                print(f"  处理数据失败: {e}")
                                continue
                    
                    # 保存完整原始数据到CSV文件，不受通达信格式限制
                    csv_path = os.path.join(CSV_INDEX_PATH, f"{market_prefix}{code}.csv")
                    import pandas as pd
                    df = pd.DataFrame(all_data)
                    # 转换日期格式
                    df['date'] = pd.to_datetime(df['datetime']).dt.strftime('%Y-%m-%d')
                    # 选择需要的列，保持与原有CSV格式一致
                    df = df[['date', 'open', 'high', 'low', 'close', 'vol', 'amount']]
                    # 添加代码列
                    df['code'] = f"{market_prefix}{code}"
                    # 重新排列列顺序
                    df = df[['date', 'code', 'open', 'high', 'low', 'close', 'vol', 'amount']]
                    # 确保目录存在
                    ensure_dir(CSV_INDEX_PATH)
                    # 保存为CSV，不包含索引
                    df.to_csv(csv_path, index=False, encoding='utf-8')
                    
                    index_updated += 1
                    index_success += 1
                    # 输出下载信息
                    operation = '下载' if mode == 'wb' else '追加'
                    print(f"  成功{operation}指数 {market_prefix}{code} 的数据，共 {new_records_count} 条")
                    print(f"  完整原始数据已保存到 {csv_path}")
                elif file_exists:
                    print(f"  指数 {market_prefix}{code} 没有新数据，跳过")
                    index_success += 1
                else:
                    print(f"  未获取到指数 {market_prefix}{code} 的数据")
                    index_fail += 1
            except Exception as e:
                print(f"  指数 {market_prefix}{code} 下载失败: {e}")
                index_fail += 1
                continue
        
        # 登出Baostock
        bs.logout()
        
        print(f"指数数据下载完成，成功: {index_success} 个，更新: {index_updated} 个，失败: {index_fail} 个")
        
        return True
        
    except Exception as e:
        print(f"下载数据时发生错误: {e}")
        return False
    finally:
        # 断开连接
        api.disconnect()

# 更新日线数据到CSV和Pickle格式
def update_daily_data():
    """更新日线数据"""
    print("\n=== 更新日线数据 ===")
    
    # 确保目标目录存在
    ensure_dir(CSV_LDAY_PATH)
    ensure_dir(PICKLE_PATH)
    
    # 遍历通达信的日线数据目录
    updated_count = 0
    
    # 处理上海A股日线数据
    sh_lday_path = os.path.join(VIPDOC_PATH, 'sh', 'lday')
    if os.path.exists(sh_lday_path):
        print(f"处理上海A股日线数据，目录: {sh_lday_path}")
        for file in os.listdir(sh_lday_path):
            if file.endswith('.day') and file.startswith('sh'):
                try:
                    # 构建目标CSV文件路径
                    csv_file = os.path.join(CSV_LDAY_PATH, file[:-4] + '.csv')
                    
                    # 检查文件是否需要更新（比较修改时间）
                    day_file = os.path.join(sh_lday_path, file)
                    if os.path.exists(csv_file):
                        day_mtime = os.path.getmtime(day_file)
                        csv_mtime = os.path.getmtime(csv_file)
                        if day_mtime <= csv_mtime:
                            continue  # 文件没有更新，跳过
                    
                    # 转换数据
                    from func import day2csv
                    day2csv(sh_lday_path, file, CSV_LDAY_PATH)
                    updated_count += 1
                except Exception as e:
                    print(f"  处理失败: {file}, 错误: {e}")
    
    # 处理深圳A股日线数据
    sz_lday_path = os.path.join(VIPDOC_PATH, 'sz', 'lday')
    if os.path.exists(sz_lday_path):
        print(f"处理深圳A股日线数据，目录: {sz_lday_path}")
        for file in os.listdir(sz_lday_path):
            if file.endswith('.day') and file.startswith('sz'):
                try:
                    # 构建目标CSV文件路径
                    csv_file = os.path.join(CSV_LDAY_PATH, file[:-4] + '.csv')
                    
                    # 检查文件是否需要更新（比较修改时间）
                    day_file = os.path.join(sz_lday_path, file)
                    if os.path.exists(csv_file):
                        day_mtime = os.path.getmtime(day_file)
                        csv_mtime = os.path.getmtime(csv_file)
                        if day_mtime <= csv_mtime:
                            continue  # 文件没有更新，跳过
                    
                    # 转换数据
                    from func import day2csv
                    day2csv(sz_lday_path, file, CSV_LDAY_PATH)
                    updated_count += 1
                except Exception as e:
                    print(f"  处理失败: {file}, 错误: {e}")
    
    print(f"日线数据处理完成，共更新 {updated_count} 个文件")
    
    # 将CSV转换为Pickle格式
    if updated_count > 0:
        print("\n将CSV数据转换为Pickle格式...")
        pickle_updated = 0
        
        for file in os.listdir(CSV_LDAY_PATH):
            if file.endswith('.csv'):
                csv_file = os.path.join(CSV_LDAY_PATH, file)
                pkl_file = os.path.join(PICKLE_PATH, file[:-4] + '.pkl')
                
                # 检查文件是否需要更新
                if os.path.exists(pkl_file):
                    csv_mtime = os.path.getmtime(csv_file)
                    pkl_mtime = os.path.getmtime(pkl_file)
                    if csv_mtime <= pkl_mtime:
                        continue  # 文件没有更新，跳过
                
                try:
                    # 读取CSV文件
                    df = pd.read_csv(csv_file, encoding='gbk')
                    
                    # 确保日期格式正确
                    if 'date' in df.columns:
                        df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
                    
                    # 保存为Pickle格式
                    df.to_pickle(pkl_file)
                    pickle_updated += 1
                except Exception as e:
                    print(f"  转换失败: {file}, 错误: {e}")
        
        print(f"Pickle格式转换完成，共更新 {pickle_updated} 个文件")

# 更新指数数据
def update_index_data():
    """更新指数数据"""
    print("\n=== 更新指数数据 ===")
    
    # 确保目标目录存在
    ensure_dir(CSV_INDEX_PATH)
    
    updated_count = 0
    
    # 处理上海指数数据
    sh_index_path = os.path.join(VIPDOC_PATH, 'sh', 'lday')
    if os.path.exists(sh_index_path):
        print(f"处理上海指数数据，目录: {sh_index_path}")
        for file in os.listdir(sh_index_path):
            if file.endswith('.day') and file.startswith('sh') and file[2:] in ['000001', '000300']:
                try:
                    # 构建目标CSV文件路径
                    csv_file = os.path.join(CSV_INDEX_PATH, file[:-4] + '.csv')
                    
                    # 检查文件是否需要更新
                    day_file = os.path.join(sh_index_path, file)
                    if os.path.exists(csv_file):
                        day_mtime = os.path.getmtime(day_file)
                        csv_mtime = os.path.getmtime(csv_file)
                        if day_mtime <= csv_mtime:
                            continue  # 文件没有更新，跳过
                    
                    # 转换数据
                    from func import day2csv
                    day2csv(sh_index_path, file, CSV_INDEX_PATH)
                    updated_count += 1
                except Exception as e:
                    print(f"  处理失败: {file}, 错误: {e}")
    
    # 处理深圳指数数据
    sz_index_path = os.path.join(VIPDOC_PATH, 'sz', 'lday')
    if os.path.exists(sz_index_path):
        print(f"处理深圳指数数据，目录: {sz_index_path}")
        for file in os.listdir(sz_index_path):
            if file.endswith('.day') and file.startswith('sz') and file[2:] in ['399001', '399006']:
                try:
                    # 构建目标CSV文件路径
                    csv_file = os.path.join(CSV_INDEX_PATH, file[:-4] + '.csv')
                    
                    # 检查文件是否需要更新
                    day_file = os.path.join(sz_index_path, file)
                    if os.path.exists(csv_file):
                        day_mtime = os.path.getmtime(day_file)
                        csv_mtime = os.path.getmtime(csv_file)
                        if day_mtime <= csv_mtime:
                            continue  # 文件没有更新，跳过
                    
                    # 转换数据
                    from func import day2csv
                    day2csv(sz_index_path, file, CSV_INDEX_PATH)
                    updated_count += 1
                except Exception as e:
                    print(f"  处理失败: {file}, 错误: {e}")
    
    print(f"指数数据处理完成，共更新 {updated_count} 个文件")

# 更新财务数据
def update_financial_data():
    """更新财务数据"""
    print("\n=== 更新财务数据 ===")
    
    # 确保目标目录存在
    ensure_dir(CSV_CW_PATH)
    
    # 通达信财务数据目录
    tdx_cw_path = os.path.join(VIPDOC_PATH, 'cw')
    if os.path.exists(tdx_cw_path):
        print(f"财务数据目录: {tdx_cw_path}")
        
        updated_count = 0
        
        # 复制所有财务数据文件到目标目录
        for file in os.listdir(tdx_cw_path):
            if file.startswith('gpcw') and (file.endswith('.dat') or file.endswith('.zip')):
                src_file = os.path.join(tdx_cw_path, file)
                dst_file = os.path.join(CSV_CW_PATH, file)
                
                # 检查文件是否需要更新
                if os.path.exists(dst_file):
                    src_mtime = os.path.getmtime(src_file)
                    dst_mtime = os.path.getmtime(dst_file)
                    if src_mtime <= dst_mtime:
                        continue  # 文件没有更新，跳过
                
                try:
                    shutil.copy2(src_file, dst_file)
                    updated_count += 1
                except Exception as e:
                    print(f"  复制失败: {file}, 错误: {e}")
        
        print(f"财务数据更新完成，共更新 {updated_count} 个文件")
    else:
        print(f"财务数据目录不存在: {tdx_cw_path}")

# 将DataFrame转换为通达信.day二进制格式
def convert_to_tdx_day_format(df, stock_code):
    """将DataFrame转换为通达信.day二进制格式"""
    if df is None or df.empty:
        return None, False
    
    # 确定市场
    market = "sh" if stock_code.startswith(('60', '688')) else "sz"
    file_path = os.path.join(VIPDOC_PATH, market, "lday", f"{market}{stock_code}.day")
    
    # 创建临时文件
    temp_file = f"{market}{stock_code}_temp.day"
    
    # 准备二进制数据
    binary_data = b''
    
    for _, row in df.iterrows():
        # 日期转换为整数 (YYYYMMDD)
        date = int(row['日期'].replace('-', ''))
        
        # 价格转换为整数 (乘以100)
        open_price = int(row['开盘'] * 100)
        high_price = int(row['最高'] * 100)
        low_price = int(row['最低'] * 100)
        close_price = int(row['收盘'] * 100)
        
        # 成交量和成交额
        volume = int(row['成交量'])
        amount = int(row['成交额'] * 100)  # 通达信中成交额单位是元*100
        
        # 写入二进制数据
        binary_data += struct.pack('<L', date)  # 日期
        binary_data += struct.pack('<L', open_price)  # 开盘价
        binary_data += struct.pack('<L', high_price)  # 最高价
        binary_data += struct.pack('<L', low_price)  # 最低价
        binary_data += struct.pack('<L', close_price)  # 收盘价
        binary_data += struct.pack('<L', volume)  # 成交量
        binary_data += struct.pack('<L', amount)  # 成交额
        binary_data += struct.pack('<L', 0)  # 保留字段
    
    # 写入临时文件
    with open(temp_file, 'wb') as f:
        f.write(binary_data)
    
    # 比较文件是否有变化
    temp_md5 = get_file_md5(temp_file)
    target_md5 = get_file_md5(file_path)
    
    if temp_md5 != target_md5:
        # 文件有变化，更新目标文件
        shutil.move(temp_file, file_path)
        return file_path, True
    else:
        # 文件没有变化，删除临时文件
        os.remove(temp_file)
        return file_path, False

# 将DataFrame转换为通达信.lc1二进制格式
def convert_to_tdx_minute_format(df, stock_code):
    """将DataFrame转换为通达信.lc1二进制格式"""
    if df is None or df.empty:
        return None, False
    
    # 确定市场
    market = "sh" if stock_code.startswith(('60', '688')) else "sz"
    file_path = os.path.join(VIPDOC_PATH, market, "fzline", f"{market}{stock_code}.lc1")
    
    # 创建临时文件
    temp_file = f"{market}{stock_code}_temp.lc1"
    
    # 准备二进制数据
    binary_data = b''
    
    for _, row in df.iterrows():
        # 日期和时间转换
        date_str, time_str = row['时间'].split(' ')
        date = int(date_str.replace('-', ''))
        time = int(time_str.replace(':', ''))
        
        # 价格转换为整数 (乘以100)
        open_price = int(row['开盘'] * 100)
        high_price = int(row['最高'] * 100)
        low_price = int(row['最低'] * 100)
        close_price = int(row['收盘'] * 100)
        
        # 成交量和成交额
        volume = int(row['成交量'])
        amount = int(row['成交额'] * 100)  # 通达信中成交额单位是元*100
        
        # 写入二进制数据
        binary_data += struct.pack('<L', date)  # 日期
        binary_data += struct.pack('<H', time)  # 时间
        binary_data += struct.pack('<H', 0)  # 保留字段
        binary_data += struct.pack('<L', open_price)  # 开盘价
        binary_data += struct.pack('<L', high_price)  # 最高价
        binary_data += struct.pack('<L', low_price)  # 最低价
        binary_data += struct.pack('<L', close_price)  # 收盘价
        binary_data += struct.pack('<L', volume)  # 成交量
        binary_data += struct.pack('<L', amount)  # 成交额
    
    # 写入临时文件
    with open(temp_file, 'wb') as f:
        f.write(binary_data)
    
    # 比较文件是否有变化
    temp_md5 = get_file_md5(temp_file)
    target_md5 = get_file_md5(file_path)
    
    if temp_md5 != target_md5:
        # 文件有变化，更新目标文件
        shutil.move(temp_file, file_path)
        return file_path, True
    else:
        # 文件没有变化，删除临时文件
        os.remove(temp_file)
        return file_path, False

# 移除了所有与akshare相关的函数，现在只使用pytdx获取数据

# 将pytdx DataFrame转换为TDX格式保存
def save_as_tdx_format_from_df(df, market, code, temp_file):
    """将pytdx返回的DataFrame转换为TDX .day格式并保存到指定临时文件"""
    from struct import pack
    import math
    
    # 处理pytdx DataFrame的列名和格式
    # pytdx返回的DataFrame列名：open, close, high, low, vol, amount, year, month, day, hour, minute, datetime
    
    # 将数据转换为TDX格式
    with open(temp_file, 'wb') as f:
        for _, row in df.iterrows():
            try:
                # 从datetime列提取日期（pytdx返回的datetime格式为YYYY-MM-DD HH:MM:SS）
                date_str = row['datetime'].split()[0].replace('-', '')
                date = int(date_str)
                
                # TDX格式要求价格乘以100后转为整数
                open_price = int(round(row['open'] * 100))
                high_price = int(round(row['high'] * 100))
                low_price = int(round(row['low'] * 100))
                close_price = int(round(row['close'] * 100))
                
                # 处理成交量（确保在unsigned int范围内）
                volume = int(row['vol'])
                # 限制volume在0到4294967295之间
                volume = max(0, min(volume, 4294967295))
                
                # 处理成交额（确保在float范围内）
                amount = row['amount']
                # 检查amount是否为有效浮点数
                if math.isnan(amount) or math.isinf(amount):
                    amount = 0.0
                else:
                    # 限制amount在float可表示的合理范围内
                    amount = max(-1e38, min(amount, 1e38))
                
                # 按照TDX格式打包数据
                # 格式说明：IIIIIfII (分别对应日期, 开盘, 最高, 最低, 收盘, 成交额, 成交量, 保留)
                data = pack('<IIIIIfII', date, open_price, high_price, low_price, close_price, amount, volume, 0)
                f.write(data)
            except Exception as e:
                print(f"  处理数据行失败: {e}")
                continue

# 更新股本变迁数据
def update_gbbq_data():
    """更新股本变迁数据"""
    print("\n=== 更新股本变迁数据 ===")
    
    # 确保目标目录存在
    ensure_dir(CSV_GBBQ_PATH)
    
    # 通达信股本变迁数据文件
    gbbq_files = ['gbbq', 'gbbq.map']
    
    for gbbq_file in gbbq_files:
        src_gbbq = os.path.join(HQ_CACHE_PATH, gbbq_file)
        
        if os.path.exists(src_gbbq):
            print(f"股本变迁数据文件: {src_gbbq}")
            
            if gbbq_file == 'gbbq':
                # 处理gbbq文件，转换为CSV格式
                dst_gbbq = os.path.join(CSV_GBBQ_PATH, 'gbbq.csv')
                
                # 检查文件是否需要更新
                need_update = True
                if os.path.exists(dst_gbbq):
                    src_mtime = os.path.getmtime(src_gbbq)
                    dst_mtime = os.path.getmtime(dst_gbbq)
                    if src_mtime <= dst_mtime:
                        need_update = False  # 文件没有更新，跳过
                
                if need_update:
                    try:
                        # 读取通达信股本变迁文件并转换为CSV格式
                        from pytdx.reader import GbbqReader
                        df = GbbqReader().get_df(src_gbbq)
                        df.to_csv(dst_gbbq, encoding='gbk', index=False)
                        print(f"  转换完成，保存到: {dst_gbbq}")
                    except Exception as e:
                        print(f"  转换失败，错误: {e}")
                else:
                    print(f"  文件没有更新，跳过")
            else:
                # 对于gbbq.map文件，直接复制
                dst_gbbq_map = os.path.join(CSV_GBBQ_PATH, gbbq_file)
                
                # 检查文件是否需要更新
                need_update = True
                if os.path.exists(dst_gbbq_map):
                    src_mtime = os.path.getmtime(src_gbbq)
                    dst_mtime = os.path.getmtime(dst_gbbq_map)
                    if src_mtime <= dst_mtime:
                        need_update = False  # 文件没有更新，跳过
                
                if need_update:
                    try:
                        shutil.copy2(src_gbbq, dst_gbbq_map)
                        print(f"  复制完成，保存到: {dst_gbbq_map}")
                    except Exception as e:
                        print(f"  复制失败，错误: {e}")
                else:
                    print(f"  文件没有更新，跳过")
        else:
            print(f"股本变迁数据文件不存在: {src_gbbq}")

# 主函数
def main():
    """主函数"""
    print("开始更新东方财富通达信盘后数据")
    print(f"通达信安装路径: {TDX_PATH}")
    print(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    
    start_time = time.time()
    
    # 1. 从网络获取最新盘后数据
    download_success = download_latest_data()
    
    end_time = time.time()
    
    print(f"\n=== 更新完成 ===")
    print(f"结束时间: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())}")
    print(f"总耗时: {end_time - start_time:.2f} 秒")
    print("数据下载完成！")

if __name__ == '__main__':
    main()
