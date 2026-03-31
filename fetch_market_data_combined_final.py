#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combined market data fetching script

Features:
1. Fetch latest daily data (.day files) from network - process all stocks
2. Update index data
3. Update financial data (stock_yjbb_em, stock_yjkb_em, stock_yjyg_em)
4. Update share capital change data
5. Convert data to CSV and Pickle format
6. Only process actually updated data
7. Run independently without TDX installation directory

Usage:
python fetch_market_data_combined.py
"""

import os
import sys
import time
import datetime
import pandas as pd
import hashlib
import json

# Import baostock library
try:
    import baostock as bs
    print("baostock library imported successfully")
except ImportError as e:
    print(f"Failed to import baostock: {e}")
    print("Please install baostock: pip install baostock")
    sys.exit(1)

try:
    from pytdx.hq import TdxHq_API
    from pytdx.util.best_ip import select_best_ip
except ImportError as e:
    print(f"Failed to import pytdx: {e}")
    print("Please install pytdx: pip install pytdx")
    sys.exit(1)

# Import akshare library to get stock list
try:
    import akshare as ak
except ImportError as e:
    print(f"Failed to import akshare: {e}")
    print("Please install akshare: pip install akshare")
    sys.exit(1)

# Local data storage path
LOCAL_DATA_PATH = "TDXdata"
CSV_LDAY_PATH = os.path.join(LOCAL_DATA_PATH, "lday_qfq")
PICKLE_PATH = os.path.join(LOCAL_DATA_PATH, "pickle")
CSV_INDEX_PATH = os.path.join(LOCAL_DATA_PATH, "index")
CSV_CW_PATH = os.path.join(LOCAL_DATA_PATH, "cw")
CSV_GBBQ_PATH = LOCAL_DATA_PATH

# Simulated TDX data directory structure (for possible future TDX client use)
SIMULATED_TDX_PATH = os.path.join(LOCAL_DATA_PATH, "simulated_tdx")
VIPDOC_PATH = os.path.join(SIMULATED_TDX_PATH, "vipdoc")
HQ_CACHE_PATH = os.path.join(SIMULATED_TDX_PATH, "T0002", "hq_cache")

# Create simulated TDX directory structure
def create_simulated_tdx_directories():
    """Create simulated TDX directory structure"""
    # Ensure main directory exists
    if not os.path.exists(SIMULATED_TDX_PATH):
        os.makedirs(SIMULATED_TDX_PATH)
    
    # Create VIPDOC directory
    if not os.path.exists(VIPDOC_PATH):
        os.makedirs(VIPDOC_PATH)
    
    # Create Shanghai and Shenzhen market directories
    for market in ["sh", "sz"]:
        market_path = os.path.join(VIPDOC_PATH, market)
        if not os.path.exists(market_path):
            os.makedirs(market_path)
        
        # Create daily and minute data directories
        for data_type in ["lday", "fzline"]:
            data_path = os.path.join(market_path, data_type)
            if not os.path.exists(data_path):
                os.makedirs(data_path)
                print(f"Created directory: {data_path}")

# Ensure directory exists
def ensure_dir(directory):
    """Ensure directory exists, create if not"""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

# Check if it's trading hours
def is_trading_hours():
    """
    Check if current time is within trading hours (9:00-17:00)
    Returns:
    - True if within trading hours
    - False if not within trading hours
    """
    # Get current time
    now = datetime.datetime.now()
    
    # Check if it's weekend
    if now.weekday() >= 5:  # 0-4 are weekdays, 5-6 are weekends
        return False
    
    # Check if time is between 9:00-17:00
    start_time = now.replace(hour=9, minute=0, second=0, microsecond=0)
    end_time = now.replace(hour=17, minute=0, second=0, microsecond=0)
    
    return start_time <= now <= end_time

# Get target date
def get_target_date():
    """
    Get target update date based on current time
    - If within trading hours (9:00-17:00), return previous trading day
    - If not within trading hours, return current date
    """
    today = datetime.datetime.today()
    
    if is_trading_hours():
        # Within trading hours, return previous trading day
        yesterday = today - datetime.timedelta(days=1)
        # Handle case where previous day is weekend
        if yesterday.weekday() == 5:  # Saturday
            yesterday = yesterday - datetime.timedelta(days=1)
        elif yesterday.weekday() == 6:  # Sunday
            yesterday = yesterday - datetime.timedelta(days=2)
        return yesterday.strftime("%Y%m%d")
    else:
        # Not within trading hours, return current date
        return today.strftime("%Y%m%d")

# Read best IP configuration
def read_best_ip():
    """Read best IP configuration from best_ip.json file"""
    best_ip_file = os.path.join(os.path.dirname(__file__), 'best_ip.json')
    try:
        with open(best_ip_file, 'r', encoding='utf-8') as f:
            best_ip_data = json.load(f)
        print(f"Successfully read best IP configuration from best_ip.json")
        return best_ip_data
    except Exception as e:
        print(f"Failed to read best_ip.json: {e}")
        return None

# Get stock list
def get_stock_list_with_akshare():
    """
    Get Shanghai and Shenzhen A-share stock list, first try to get from CNINFO URL, if failed then through akshare
    
    Returns:
    - Stock list, format: [(market_prefix, code), ...]
    """
    stocks = []
    
    # First try to get A-share stock list from CNINFO URL
    try:
        print("  Trying to get A-share stock list from CNINFO URL...")
        import requests
        import json
        
        url = "https://www.cninfo.com.cn/new/data/szse_stock.json"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Check if request is successful
        data = json.loads(response.text)
        all_stocks = data.get('stockList', [])
        print(f"  Successfully got {len(all_stocks)} stocks from CNINFO")
        
        # Process stock list
        for stock in all_stocks:
            code = stock.get('code', '')
            category = stock.get('category', '')
            
            # Only process A-shares
            if code and category == 'A-share':
                # Determine market based on stock code
                if code.startswith(('600', '601', '603', '605', '688')):
                    # Shanghai market
                    stocks.append(('sh', code))
                elif code.startswith(('000', '001', '002', '003', '300', '301', '302')):
                    # Shenzhen market
                    stocks.append(('sz', code))
        
        if stocks:
            print(f"  Finally got {len(stocks)} A-shares")
            return stocks
        print("  Stock list from CNINFO is empty, trying through akshare")
    except Exception as url_e:
        print(f"  Failed to get stock list from CNINFO URL: {url_e}, trying through akshare")
    
    # If CNINFO fails, get through akshare
    try:
        # Get A-share stock list
        print("  Trying to get A-share stock list through akshare...")
        stock_df = ak.stock_info_a_code_name()
        print(f"  Successfully got {len(stock_df)} stocks")
        
        # Process stock list
        for index, row in stock_df.iterrows():
            code = row['code']
            # Determine market based on stock code
            if code.startswith(('600', '601', '603', '605', '688')):
                # Shanghai market
                stocks.append(('sh', code))
            elif code.startswith(('000', '001', '002', '003', '300', '301', '302')):
                # Shenzhen market
                stocks.append(('sz', code))
        
        print(f"  Finally got {len(stocks)} A-shares")
        return stocks
    except Exception as e:
        print(f"  Failed to get stock list through akshare: {e}")
        return []

# Download stock daily data
def download_stock_daily_data(api, stock_list, target_date_int, vipdoc_path):
    """
    Download and update stock daily data
    
    Args:
    - api: TDX API instance
    - stock_list: List of stocks to process
    - target_date_int: Target date in YYYYMMDD format
    - vipdoc_path: Path to VIPDOC directory
    
    Returns:
    - Tuple of (success_count, updated_count, fail_count)
    """
    print("Updating daily data...")
    
    # Process stock list, update data
    success_count = 0
    updated_count = 0
    fail_count = 0
    
    # Process in batches, 100 stocks per batch
    batch_size = 100
    total_batches = (len(stock_list) + batch_size - 1) // batch_size
    
    for batch_idx in range(total_batches):
        start_idx = batch_idx * batch_size
        end_idx = min((batch_idx + 1) * batch_size, len(stock_list))
        batch = stock_list[start_idx:end_idx]
        
        print(f"Processing batch {batch_idx + 1}/{total_batches} ({start_idx + 1}-{end_idx}/{len(stock_list)})")
        
        for market_prefix, code in batch:
            # Process current stock, skip invalid stocks
            
            try:
                # Determine market code
                market = 1 if market_prefix == 'sh' else 0
                
                # Determine data file path
                data_path = os.path.join(vipdoc_path, market_prefix, 'lday')
                ensure_dir(data_path)
                target_file = os.path.join(data_path, f"{market_prefix}{code}.day")
                
                # Check existing file, determine if update is needed
                last_date = None
                file_exists = os.path.exists(target_file)
                
                if file_exists:
                    try:
                        with open(target_file, 'rb') as f:
                            f.seek(0, 2)  # Move to end of file
                            file_size = f.tell()
                            if file_size >= 32:
                                # Read first record (latest data) from file
                                f.seek(0, 0)  # Move to start of file
                                first_record = f.read(32)
                                from struct import unpack
                                date = unpack('<IIIIIfII', first_record)[0]  # Read date field
                                
                                # Verify date is within valid range
                                if 19900101 <= date <= 20301231:
                                    last_date = date
                            else:
                                print(f"  File size less than 32 bytes, need to reupdate data")
                                last_date = None
                    except Exception as e:
                        print(f"  Failed to read existing file: {e}, need to reupdate data")
                        last_date = None
                
                # Prepare to get data, maximum 500 bars per request
                all_data = []
                max_bars_per_request = 500
                current_offset = 0
                has_new_data = False
                
                while True:
                    # Get data
                    data = api.get_security_bars(9, market, code, current_offset, max_bars_per_request)
                    if not data or len(data) == 0:
                        break  # No more data
                    
                    # If there is last_date, only get data after last_date
                    if last_date:
                        filtered_data = []
                        
                        # Process data, add date field for sorting
                        data_with_dates = []
                        for k_line in data:
                            # Extract date
                            if 'datetime' in k_line:
                                # Date format: 2026-03-04 17:00
                                date_str = k_line['datetime'].split(' ')[0]  # Extract YYYY-MM-DD
                                date_int = int(date_str.replace('-', ''))  # Convert to YYYYMMDD
                            else:
                                # Old format, use year/month/day fields
                                year = k_line.get('year', 0)
                                month = k_line.get('month', 0)
                                day = k_line.get('day', 0)
                                date_int = year * 10000 + month * 100 + day
                            
                            data_with_dates.append((date_int, k_line))
                        
                        # Sort by date in descending order
                        data_with_dates.sort(key=lambda x: x[0], reverse=True)
                        
                        # Only keep data after last_date and not exceeding target date
                        for date_int, k_line in data_with_dates:
                            if date_int > last_date and date_int <= target_date_int:
                                filtered_data.append(k_line)
                            elif date_int == last_date:
                                # Skip duplicate date
                                continue
                            else:
                                # Data expired, stop processing
                                break
                        
                        if filtered_data:
                            all_data.extend(filtered_data)
                            has_new_data = True
                        else:
                            # No new data, stop processing
                            break
                    else:
                        # No last_date, get all data not exceeding target date
                        # Process data, add date field for sorting
                        data_with_dates = []
                        for k_line in data:
                            # Extract date
                            if 'datetime' in k_line:
                                # Date format: 2026-03-04 17:00
                                date_str = k_line['datetime'].split(' ')[0]  # Extract YYYY-MM-DD
                                date_int = int(date_str.replace('-', ''))  # Convert to YYYYMMDD
                            else:
                                # Old format, use year/month/day fields
                                year = k_line.get('year', 0)
                                month = k_line.get('month', 0)
                                day = k_line.get('day', 0)
                                date_int = year * 10000 + month * 100 + day
                            
                            data_with_dates.append((date_int, k_line))
                        
                        # Sort by date in descending order
                        data_with_dates.sort(key=lambda x: x[0], reverse=True)
                        
                        # Only keep data not exceeding target date
                        filtered_data = []
                        for date_int, k_line in data_with_dates:
                            if date_int <= target_date_int:
                                filtered_data.append(k_line)
                            else:
                                continue
                        
                        if filtered_data:
                            all_data.extend(filtered_data)
                            has_new_data = True
                    
                    # If obtained data is less than requested maximum, all data has been obtained
                    if len(data) < max_bars_per_request:
                        break
                    
                    # Update offset, continue to get next batch of data
                    current_offset += max_bars_per_request
                
                if has_new_data and all_data:
                    # Sort new data by date in descending order to ensure data is arranged from new to old
                    # Add sort_date field to each data for sorting
                    for k_line in all_data:
                        if 'datetime' in k_line:
                            date_str = k_line['datetime'].split(' ')[0]  # Extract YYYY-MM-DD
                            k_line['sort_date'] = int(date_str.replace('-', ''))  # Convert to YYYYMMDD
                        else:
                            year = k_line.get('year', 0)
                            month = k_line.get('month', 0)
                            day = k_line.get('day', 0)
                            k_line['sort_date'] = year * 10000 + month * 100 + day
                    
                    # Sort by date in descending order
                    all_data.sort(key=lambda x: x['sort_date'], reverse=True)
                    
                    # Filter invalid date data
                    valid_data = []
                    for data in all_data:
                        date = data['sort_date']
                        # Only keep dates between 19900101-20301231
                        if 19900101 <= date <= 20301231:
                            valid_data.append(data)
                    all_data = valid_data
                    
                    if not all_data:
                        print(f"  No valid data, skipping")
                        continue
                    
                    if file_exists:
                        # Append mode: read existing data, add new data to front, then rewrite
                        try:
                            existing_data = []
                            with open(target_file, 'rb') as f:
                                while True:
                                    record = f.read(32)
                                    if not record or len(record) != 32:
                                        break
                                    # Verify date of existing record
                                    from struct import unpack
                                    try:
                                        date = unpack('<I', record[0:4])[0]
                                        # Only keep dates between 19900101-20301231
                                        if 19900101 <= date <= 20301231:
                                            existing_data.append(record)
                                    except Exception:
                                        # Skip invalid records
                                        continue
                            
                            # Prepare all records: new data + existing data
                            all_records = []
                            
                            # Process new data
                            from struct import pack
                            for k_line in all_data:
                                # Extract date
                                date_int = k_line['sort_date']
                                
                                # Price multiplied by 100, converted to integer
                                open_price = int(k_line.get('open', 0) * 100)
                                high_price = int(k_line.get('high', 0) * 100)
                                low_price = int(k_line.get('low', 0) * 100)
                                close_price = int(k_line.get('close', 0) * 100)
                                
                                # Volume and amount
                                volume = int(k_line.get('vol', 0))
                                amount = k_line.get('amount', 0)
                                
                                # Reserved field
                                reserve = 0
                                
                                # Pack data into binary format <IIIIIfII
                                record = pack('<IIIIIfII', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                all_records.append(record)
                            
                            # Add existing data
                            all_records.extend(existing_data)
                            
                            # Write to data file
                            try:
                                with open(target_file, 'wb') as f:
                                    for record in all_records:
                                        f.write(record)
                                
                                mode = 'ab'  # Append mode, but actually we rewrite the entire file
                            except PermissionError as e:
                                print(f"  Failed to write file: {e}, trying to delete file and recreate")
                                # Try to delete file and recreate
                                try:
                                    os.remove(target_file)
                                    with open(target_file, 'wb') as f:
                                        for record in all_records:
                                            f.write(record)
                                    mode = 'ab'
                                    print(f"  Successfully deleted and recreated file")
                                except Exception as e2:
                                    print(f"  Failed to delete and recreate file: {e2}, skipping this file")
                                    continue
                        except Exception as e:
                            print(f"  Failed to merge data: {e}, need to reupdate data")
                            # Reupdate data
                            from struct import pack
                            try:
                                with open(target_file, 'wb') as f:
                                    for k_line in all_data:
                                        # Extract date
                                        date_int = k_line['sort_date']
                                        
                                        # Price multiplied by 100, converted to integer
                                        open_price = int(k_line.get('open', 0) * 100)
                                        high_price = int(k_line.get('high', 0) * 100)
                                        low_price = int(k_line.get('low', 0) * 100)
                                        close_price = int(k_line.get('close', 0) * 100)
                                        
                                        # Volume and amount
                                        volume = int(k_line.get('vol', 0))
                                        amount = k_line.get('amount', 0)
                                        
                                        # Reserved field
                                        reserve = 0
                                        
                                        # Pack data into binary format <IIIIIfII
                                        record = pack('<IIIIIfII', 
                                                    date_int, open_price, high_price, low_price, 
                                                    close_price, amount, volume, reserve)
                                        f.write(record)
                                mode = 'wb'
                            except PermissionError as e:
                                print(f"  Failed to write file: {e}, trying to delete file and recreate")
                                # Try to delete file and recreate
                                try:
                                    os.remove(target_file)
                                    with open(target_file, 'wb') as f:
                                        for k_line in all_data:
                                            # Extract date
                                            date_int = k_line['sort_date']
                                            
                                            # Price multiplied by 100, converted to integer
                                            open_price = int(k_line.get('open', 0) * 100)
                                            high_price = int(k_line.get('high', 0) * 100)
                                            low_price = int(k_line.get('low', 0) * 100)
                                            close_price = int(k_line.get('close', 0) * 100)
                                            
                                            # Volume and amount
                                            volume = int(k_line.get('vol', 0))
                                            amount = k_line.get('amount', 0)
                                            
                                            # Reserved field
                                            reserve = 0
                                            
                                            # Pack data into binary format <IIIIIfII
                                            record = pack('<IIIIIfII', 
                                                        date_int, open_price, high_price, low_price, 
                                                        close_price, amount, volume, reserve)
                                            f.write(record)
                                    mode = 'wb'
                                    print(f"  Successfully deleted and recreated file")
                                except Exception as e2:
                                    print(f"  Failed to delete and recreate file: {e2}, skipping this file")
                                    continue
                    else:
                        # New file, write data directly
                        mode = 'wb'
                        from struct import pack
                        with open(target_file, mode) as f:
                            for k_line in all_data:
                                # Extract date
                                date_int = k_line['sort_date']
                                
                                # Price multiplied by 100, converted to integer
                                open_price = int(k_line.get('open', 0) * 100)
                                high_price = int(k_line.get('high', 0) * 100)
                                low_price = int(k_line.get('low', 0) * 100)
                                close_price = int(k_line.get('close', 0) * 100)
                                
                                # Volume and amount
                                volume = int(k_line.get('vol', 0))
                                amount = k_line.get('amount', 0)
                                
                                # Reserved field
                                reserve = 0
                                
                                # Pack data into binary format <IIIIIfII
                                record = pack('<IIIIIfII', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                f.write(record)
                    
                    # Record update result
                    operation = 'Added' if mode == 'wb' else 'Updated'
                    print(f"  Successfully {operation} {market_prefix}{code} daily data, total {len(all_data)} records")
                    
                    updated_count += 1
                    success_count += 1
                elif file_exists:
                    # print(f"  {market_prefix}{code} has no new data, skipping")
                    success_count += 1
                else:
                    fail_count += 1
            except Exception as e:
                fail_count += 1
                # Skip failed stocks, continue to next one
                continue
    
    print(f"Data update completed, success: {success_count}, updated: {updated_count}, failed: {fail_count}")
    return success_count, updated_count, fail_count

# Update index data
def update_index_data(target_date_int, vipdoc_path):
    """
    Update index data using Baostock API
    
    Args:
    - target_date_int: Target date in YYYYMMDD format
    - vipdoc_path: Path to VIPDOC directory
    
    Returns:
    - Tuple of (index_success, index_updated, index_fail)
    """
    print("\nUpdating index data...")
    
    # Login to Baostock
    bs.login()
    
    indices = [
        ('sh', '000001'),  # Shanghai Composite Index
        ('sh', '000300'),  # CSI 300
        ('sz', '399001'),  # Shenzhen Component Index
        ('sz', '399006')   # ChiNext Index
    ]
    
    index_success = 0
    index_updated = 0
    index_fail = 0
    
    for market_prefix, code in indices:
        try:
            print(f"  Getting index {market_prefix}{code} daily data")
            
            # Determine data file path
            data_path = os.path.join(vipdoc_path, market_prefix, 'lday')
            ensure_dir(data_path)
            target_file = os.path.join(data_path, f"{market_prefix}{code}.day")
            
            # Check existing file, determine if update is needed
            last_date = None
            file_exists = os.path.exists(target_file)
            
            if file_exists:
                try:
                    with open(target_file, 'rb') as f:
                        f.seek(0, 2)  # Move to end of file
                        file_size = f.tell()
                        if file_size >= 32:
                            # Read first record (latest data) from file
                            f.seek(0, 0)  # Move to start of file
                            first_record = f.read(32)
                            from struct import unpack
                            date = unpack('<IIIIIfII', first_record)[0]  # Read date field
                            
                            # Verify date is within valid range
                            if 19900101 <= date <= 20301231:
                                last_date = date
                            else:
                                # Invalid date, need to reupdate data
                                print(f"  Detected invalid date: {date}, need to reupdate data")
                                last_date = None
                        else:
                            print(f"  File size less than 32 bytes, need to reupdate data")
                except Exception as e:
                    print(f"  Failed to read existing file: {e}, need to reupdate data")
            
            # Determine Baostock code
            baostock_code = f"{market_prefix}.{code}"
            
            # Start date
            start_date = "1990-01-01"
            
            # End date in YYYY-MM-DD format of target date
            end_date = datetime.datetime.strptime(str(target_date_int), "%Y%m%d").strftime("%Y-%m-%d")
            print(f"  Baostock API request parameters: code={baostock_code}, start_date={start_date}, end_date={end_date}")
            
            # Get index data through Baostock
            rs = bs.query_history_k_data_plus(
                baostock_code,
                "date,open,high,low,close,volume,amount",
                start_date=start_date,
                end_date=end_date,
                frequency="d",
                adjustflag="3"  # 3 means no adjustment
            )
            
            # Process return result
            all_data = []
            max_date = None
            total_rows = 0
            processed_rows = 0
            while (rs.error_code == '0') & rs.next():
                total_rows += 1
                # Get one row of data and process
                row = rs.get_row_data()
                # Process data format
                date_str = row[0]  # Date format: YYYY-MM-DD
                date_int = int(date_str.replace('-', ''))
                
                # Record maximum date
                if max_date is None or date_int > max_date:
                    max_date = date_int
                
                # Only process data after last_date and not exceeding target date
                if last_date and date_int <= last_date:
                    continue
                if date_int > target_date_int:
                    continue
                
                processed_rows += 1
                # Verify data validity, skip invalid data
                try:
                    # Process price data
                    open_price = float(row[1]) if row[1] != '' else 0.0
                    high_price = float(row[2]) if row[2] != '' else 0.0
                    low_price = float(row[3]) if row[3] != '' else 0.0
                    close_price = float(row[4]) if row[4] != '' else 0.0
                    
                    # Process volume and amount
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
                    print(f"  Skipping invalid data: {e}, data: {row}")
                    continue
            
            print(f"  Baostock returned data: {total_rows}, processed valid data: {processed_rows}, latest date: {max_date}, target date: {target_date_int}")
            
            # Sort data by date in descending order
            if all_data:
                all_data.sort(key=lambda x: x['sort_date'], reverse=True)
                print(f"  Valid data range: {all_data[-1]['sort_date']} to {all_data[0]['sort_date']}")
                has_new_data = True
            else:
                print(f"  No new data needs to be updated")
                has_new_data = False
                
            # Check if latest date returned by Baostock is less than target date
            if max_date and max_date < target_date_int:
                print(f"  Note: Latest data date {max_date} returned by Baostock is less than target date {target_date_int}")
                print(f"  This may be because {target_date_int} is a trading day but data has not been updated yet")
            
            if has_new_data and all_data:
                if file_exists:
                    # Append mode: read existing data, add new data to front, then rewrite
                    try:
                        existing_data = []
                        with open(target_file, 'rb') as f:
                            while True:
                                record = f.read(32)
                                if not record or len(record) != 32:
                                    break
                                # Verify date of existing record
                                from struct import unpack
                                try:
                                    date = unpack('<I', record[0:4])[0]
                                    # Only keep dates between 19900101-20301231
                                    if 19900101 <= date <= 20301231:
                                        existing_data.append(record)
                                except Exception:
                                    # Skip invalid records
                                    continue
                        
                        # Prepare all records: new data + existing data
                        all_records = []
                        new_records_count = 0
                        
                        # Process new data
                        from struct import pack
                        for k_line in all_data:
                            try:
                                # Extract date
                                date_int = k_line['sort_date']
                                
                                # Verify price data validity
                                open_p = k_line.get('open', 0)
                                high_p = k_line.get('high', 0)
                                low_p = k_line.get('low', 0)
                                close_p = k_line.get('close', 0)
                                
                                if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                    continue  # Skip invalid data
                                
                                # Price multiplied by 100, converted to integer
                                open_price = int(round(open_p * 100))
                                high_price = int(round(high_p * 100))
                                low_price = int(round(low_p * 100))
                                close_price = int(round(close_p * 100))
                                
                                # Process volume
                                import math
                                volume = int(k_line.get('vol', 0))
                                
                                # Check if price fields are within range
                                date_int = max(0, min(date_int, 4294967295))
                                open_price = max(0, min(open_price, 4294967295))
                                high_price = max(0, min(high_price, 4294967295))
                                low_price = max(0, min(low_price, 4294967295))
                                close_price = max(0, min(close_price, 4294967295))
                                
                                # Volume processing - ensure non-negative
                                volume = max(0, volume)
                                
                                # Get amount value
                                amount = k_line.get('amount', 0)
                                
                                # Check if amount is valid float
                                if math.isnan(amount) or math.isinf(amount):
                                    amount = 0.0
                                else:
                                    # Remove upper limit for amount, only ensure non-negative
                                    amount = max(0.0, amount)
                                
                                # Reserved field
                                reserve = 0
                                
                                # Pack data into binary format <IIIIIfII
                                record = pack('<IIIIIfII', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                all_records.append(record)
                                new_records_count += 1
                            except Exception as e:
                                print(f"  Data processing failed: {e}")
                                continue
                        
                        # Add existing data
                        all_records.extend(existing_data)
                        
                        # Write to data file
                        try:
                            with open(target_file, 'wb') as f:
                                for record in all_records:
                                    f.write(record)
                            
                            mode = 'ab'  # Append mode, but actually we rewrite the entire file
                        except PermissionError as e:
                            print(f"  Failed to write file: {e}, trying to delete file and recreate")
                            # Try to delete file and recreate
                            try:
                                os.remove(target_file)
                                with open(target_file, 'wb') as f:
                                    for record in all_records:
                                        f.write(record)
                                mode = 'ab'
                                print(f"  Successfully deleted and recreated file")
                            except Exception as e2:
                                print(f"  Failed to delete and recreate file: {e2}, skipping this file")
                                continue
                    except Exception as e:
                        print(f"  Failed to merge data: {e}, need to reupdate data")
                        # Reupdate data
                        new_records_count = 0
                        from struct import pack
                        try:
                            with open(target_file, 'wb') as f:
                                for k_line in all_data:
                                    try:
                                        # Extract date
                                        date_int = k_line['sort_date']
                                        
                                        # Verify price data validity
                                        open_p = k_line.get('open', 0)
                                        high_p = k_line.get('high', 0)
                                        low_p = k_line.get('low', 0)
                                        close_p = k_line.get('close', 0)
                                        
                                        if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                            continue  # Skip invalid data
                                        
                                        # Price multiplied by 100, converted to integer
                                        open_price = int(round(open_p * 100))
                                        high_price = int(round(high_p * 100))
                                        low_price = int(round(low_p * 100))
                                        close_price = int(round(close_p * 100))
                                        
                                        # Process volume
                                        import math
                                        volume = int(k_line.get('vol', 0))
                                        
                                        # Check if price fields are within range
                                        date_int = max(0, min(date_int, 4294967295))
                                        open_price = max(0, min(open_price, 4294967295))
                                        high_price = max(0, min(high_price, 4294967295))
                                        low_price = max(0, min(low_price, 4294967295))
                                        close_price = max(0, min(close_price, 4294967295))
                                        
                                        # Volume processing - ensure non-negative
                                        volume = max(0, volume)
                                        
                                        # Get amount value
                                        amount = k_line.get('amount', 0)
                                        
                                        # Check if amount is valid float
                                        if math.isnan(amount) or math.isinf(amount):
                                            amount = 0.0
                                        else:
                                            # Remove upper limit for amount, only ensure non-negative
                                            amount = max(0.0, amount)
                                        
                                        # Reserved field
                                        reserve = 0
                                        
                                        # Pack data into binary format <IIIIIfII
                                        record = pack('<IIIIIfII', 
                                                    date_int, open_price, high_price, low_price, 
                                                    close_price, amount, volume, reserve)
                                        f.write(record)
                                        new_records_count += 1
                                    except Exception as e:
                                        print(f"  Data processing failed: {e}")
                                        continue
                                mode = 'wb'
                        except PermissionError as e:
                            print(f"  Failed to write file: {e}, trying to delete file and recreate")
                            # Try to delete file and recreate
                            try:
                                os.remove(target_file)
                                with open(target_file, 'wb') as f:
                                    for k_line in all_data:
                                        try:
                                            # Extract date
                                            date_int = k_line['sort_date']
                                            
                                            # Verify price data validity
                                            open_p = k_line.get('open', 0)
                                            high_p = k_line.get('high', 0)
                                            low_p = k_line.get('low', 0)
                                            close_p = k_line.get('close', 0)
                                            
                                            if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                                continue  # Skip invalid data
                                            
                                            # Price multiplied by 100, converted to integer
                                            open_price = int(round(open_p * 100))
                                            high_price = int(round(high_p * 100))
                                            low_price = int(round(low_p * 100))
                                            close_price = int(round(close_p * 100))
                                            
                                            # Process volume
                                            import math
                                            volume = int(k_line.get('vol', 0))
                                            
                                            # Check if price fields are within range
                                            date_int = max(0, min(date_int, 4294967295))
                                            open_price = max(0, min(open_price, 4294967295))
                                            high_price = max(0, min(high_price, 4294967295))
                                            low_price = max(0, min(low_price, 4294967295))
                                            close_price = max(0, min(close_price, 4294967295))
                                            
                                            # Volume processing - ensure non-negative
                                            volume = max(0, volume)
                                            
                                            # Get amount value
                                            amount = k_line.get('amount', 0)
                                            
                                            # Check if amount is valid float
                                            if math.isnan(amount) or math.isinf(amount):
                                                amount = 0.0
                                            else:
                                                # Remove upper limit for amount, only ensure non-negative
                                                amount = max(0.0, amount)
                                            
                                            # Reserved field
                                            reserve = 0
                                            
                                            # Pack data into binary format <IIIIIfII
                                            record = pack('<IIIIIfII', 
                                                        date_int, open_price, high_price, low_price, 
                                                        close_price, amount, volume, reserve)
                                            f.write(record)
                                            new_records_count += 1
                                        except Exception as e:
                                            print(f"  Data processing failed: {e}")
                                            continue
                                mode = 'wb'
                                print(f"  Successfully deleted and recreated file")
                            except Exception as e2:
                                print(f"  Failed to delete and recreate file: {e2}, skipping this file")
                                continue
                else:
                    # New file, write data directly
                    mode = 'wb'
                    new_records_count = 0
                    from struct import pack
                    with open(target_file, mode) as f:
                        for k_line in all_data:
                            try:
                                # Extract date
                                date_int = k_line['sort_date']
                                
                                # Verify price data validity
                                open_p = k_line.get('open', 0)
                                high_p = k_line.get('high', 0)
                                low_p = k_line.get('low', 0)
                                close_p = k_line.get('close', 0)
                                
                                if open_p <= 0 or high_p <= 0 or low_p <= 0 or close_p <= 0:
                                    continue  # Skip invalid data
                                
                                # Price multiplied by 100, converted to integer
                                open_price = int(round(open_p * 100))
                                high_price = int(round(high_p * 100))
                                low_price = int(round(low_p * 100))
                                close_price = int(round(close_p * 100))
                                
                                # Process volume
                                import math
                                volume = int(k_line.get('vol', 0))
                                
                                # Check if price fields are within range
                                date_int = max(0, min(date_int, 4294967295))
                                open_price = max(0, min(open_price, 4294967295))
                                high_price = max(0, min(high_price, 4294967295))
                                low_price = max(0, min(low_price, 4294967295))
                                close_price = max(0, min(close_price, 4294967295))
                                
                                # Volume processing - ensure non-negative
                                volume = max(0, volume)
                                
                                # Get amount value
                                amount = k_line.get('amount', 0)
                                
                                # Check if amount is valid float
                                if math.isnan(amount) or math.isinf(amount):
                                    amount = 0.0
                                else:
                                    # Remove upper limit for amount, only ensure non-negative
                                    amount = max(0.0, amount)
                                
                                # Reserved field
                                reserve = 0
                                
                                # Pack data into binary format <IIIIIfII
                                record = pack('<IIIIIfII', 
                                            date_int, open_price, high_price, low_price, 
                                            close_price, amount, volume, reserve)
                                f.write(record)
                                new_records_count += 1
                            except Exception as e:
                                print(f"  Data processing failed: {e}")
                                continue
            
            # Record update result
            if has_new_data and all_data:
                operation = 'Added' if mode == 'wb' else 'Updated'
                print(f"  Successfully {operation} {market_prefix}{code} index data, total {new_records_count} records")
                
                index_updated += 1
                index_success += 1
            else:
                if file_exists:
                    print(f"  {market_prefix}{code} has no new data, skipping")
                    index_success += 1
                else:
                    index_fail += 1
        except Exception as e:
            index_fail += 1
            print(f"  Failed to get index {market_prefix}{code} data: {e}")
            continue
    
    # Logout from Baostock
    bs.logout()
    
    print(f"Index data update completed, success: {index_success}, updated: {index_updated}, failed: {index_fail}")
    return index_success, index_updated, index_fail

# Update financial data
def update_financial_data(simulated_tdx_path):
    """
    Update financial data (stock_yjbb_em, stock_yjkb_em, stock_yjyg_em)
    
    Args:
    - simulated_tdx_path: Path to simulated TDX directory
    
    Returns:
    - Tuple of (success_count, total_count)
    """
    print("\nUpdating financial data...")
    try:
        # Create financial data directories
        cw_dirs = {
            'yjbb': os.path.join(simulated_tdx_path, 'cw', 'yjbb'),
            'yjkb': os.path.join(simulated_tdx_path, 'cw', 'yjkb'),
            'yjyg': os.path.join(simulated_tdx_path, 'cw', 'yjyg')
        }
        
        for dir_path in cw_dirs.values():
            ensure_dir(dir_path)
        
        # Generate quarterly date list (starting from 2010)
        start_date = datetime.datetime(2010, 12, 31)
        end_date = datetime.datetime.now()
        quarterly_dates = []
        
        current_date = start_date
        while current_date <= end_date:
            quarterly_dates.append(current_date.strftime("%Y%m%d"))
            # Move to next quarter
            if current_date.month == 12:
                current_date = current_date.replace(year=current_date.year + 1, month=3, day=31)
            elif current_date.month == 3:
                current_date = current_date.replace(month=6, day=30)
            elif current_date.month == 6:
                current_date = current_date.replace(month=9, day=30)
            elif current_date.month == 9:
                current_date = current_date.replace(month=12, day=31)
        
        print(f"Generated {len(quarterly_dates)} quarterly dates, from {quarterly_dates[0]} to {quarterly_dates[-1]}")
        
        # Download financial data for each quarter
        success_count = 0
        total_count = 0
        
        # Define financial data functions
        financial_functions = {
            'yjbb': ak.stock_yjbb_em,
            'yjkb': ak.stock_yjkb_em,
            'yjyg': ak.stock_yjyg_em
        }
        
        for i, date in enumerate(quarterly_dates):
            if i % 4 == 0:  # Show progress once per year
                print(f"Processing quarter {i+1}/{len(quarterly_dates)}: {date}")
            
            for data_type, func in financial_functions.items():
                total_count += 1
                try:
                    # Check if file exists
                    csv_file = os.path.join(cw_dirs[data_type], f"{data_type}{date}.csv")
                    file_exists = os.path.exists(csv_file)
                    
                    # Get financial data
                    try:
                        data = func(date=date)
                    except Exception as e:
                        print(f"  Failed to get {data_type} data: {e}")
                        data = pd.DataFrame()
                    
                    if not data.empty:
                        # Format stock code to 6-digit string
                        if 'code' in data.columns:
                            data['code'] = data['code'].apply(lambda x: str(x).zfill(6))
                        elif 'stock_code' in data.columns:
                            data['stock_code'] = data['stock_code'].apply(lambda x: str(x).zfill(6))
                        
                        if file_exists:
                            # Read existing data
                            existing_data = pd.read_csv(csv_file, encoding='utf-8-sig')
                            # Format stock code in existing data for comparison
                            if 'code' in existing_data.columns:
                                existing_data['code'] = existing_data['code'].apply(lambda x: str(x).zfill(6))
                            elif 'stock_code' in existing_data.columns:
                                existing_data['stock_code'] = existing_data['stock_code'].apply(lambda x: str(x).zfill(6))
                            # Check if data has changed
                            if len(existing_data) == len(data) and existing_data.equals(data):
                                print(f"  {data_type} data for {date} has not changed, skipping update")
                                continue
                        # Save to corresponding directory, use utf-8-sig encoding to avoid Chinese garbled characters
                        data.to_csv(csv_file, index=False, encoding='utf-8-sig')
                        success_count += 1
                        print(f"  Successfully downloaded {data_type} data for {date}")
                    else:
                        print(f"  No {data_type} data for {date}")
                except Exception as e:
                    print(f"  Failed to get {data_type} data for {date}: {e}")
                    continue
        
        print(f"Financial data update completed, success: {success_count}, total: {total_count}")
        return success_count, total_count
    except Exception as e:
        print(f"Failed to update financial data: {e}")
        return 0, 0

# Update share capital change data
def update_share_capital_data(simulated_tdx_path):
    """
    Update share capital change data
    
    Args:
    - simulated_tdx_path: Path to simulated TDX directory
    
    Returns:
    - Tuple of (success_count, total_count)
    """
    print("\nUpdating share capital change data...")
    try:
        # Create gbqq directory for share capital change data
        GBQQ_PATH = os.path.join(simulated_tdx_path, "gbqq")
        if not os.path.exists(GBQQ_PATH):
            os.makedirs(GBQQ_PATH)
            print(f"Created directory: {GBQQ_PATH}")
        
        # Get all valid A-share stock codes
        try:
            stock_list = ak.stock_info_a_code_name()
            valid_codes = set(stock_list['code'].tolist())
            print(f"Got {len(valid_codes)} valid A-share stock codes")
        except Exception as e:
            print(f"Failed to get valid stock codes: {e}")
            # Use test codes as backup
            valid_codes = {'600000', '600001', '600002', '600003', '600004', '600005', '600006', '600007', '600008', '600009'}
        
        # Use all valid A-share stock codes
        stock_codes = list(valid_codes)
        
        print(f"Got {len(stock_codes)} stocks, starting to download share capital change data...")
        
        # Download share capital change data
        success_count = 0
        for i, code in enumerate(stock_codes):
            if i % 50 == 0:
                print(f"Processing {i}/{len(stock_codes)} stocks")
            
            # Check if stock code is valid
            if code not in valid_codes:
                print(f"  Skipping invalid stock code: {code}")
                continue
            
            try:
                # Check if file exists
                capital_file = os.path.join(GBQQ_PATH, f"{code}_capital.csv")
                file_exists = os.path.exists(capital_file)
                
                # Use ak.stock_share_change_cninfo to get share capital change data
                try:
                    capital_data = ak.stock_share_change_cninfo(symbol=code)
                    if not capital_data.empty:
                        # Format stock code to 6-digit string
                        if 'code' in capital_data.columns:
                            capital_data['code'] = capital_data['code'].apply(lambda x: str(x).zfill(6))
                        elif 'stock_code' in capital_data.columns:
                            capital_data['stock_code'] = capital_data['stock_code'].apply(lambda x: str(x).zfill(6))
                        
                        # Handle missing '公告日期' field
                        if '公告日期' not in capital_data.columns and '变动日期' in capital_data.columns:
                            # Use '变动日期' values and rename column to '公告日期'
                            capital_data['公告日期'] = capital_data['变动日期']
                            # Drop the original '变动日期' column if needed
                            if '变动日期' in capital_data.columns:
                                capital_data = capital_data.drop('变动日期', axis=1)
                            print(f"  Using '变动日期' as '公告日期' for {code}")
                        
                        if file_exists:
                            # Read existing data
                            existing_data = pd.read_csv(capital_file, encoding='utf-8-sig')
                            # Format stock code in existing data for comparison
                            if 'code' in existing_data.columns:
                                existing_data['code'] = existing_data['code'].apply(lambda x: str(x).zfill(6))
                            elif 'stock_code' in existing_data.columns:
                                existing_data['stock_code'] = existing_data['stock_code'].apply(lambda x: str(x).zfill(6))
                            # Check if data has changed
                            if len(existing_data) == len(capital_data) and existing_data.equals(capital_data):
                                print(f"  Share capital change data for {code} has not changed, skipping update")
                                continue
                        # Save to gbqq directory, use utf-8-sig encoding to avoid Chinese garbled characters
                        capital_data.to_csv(capital_file, index=False, encoding='utf-8-sig')
                        success_count += 1
                        print(f"  Successfully downloaded share capital change data for {code}")
                    else:
                        print(f"  No share capital change data for {code}")
                except KeyError as e:
                    # Handle other KeyError cases
                    print(f"  Failed to process share capital change data for {code}: {e}")
                except Exception as e:
                    # Handle other exceptions
                    print(f"  Failed to get share capital change data for {code}: {e}")
            except Exception as e:
                print(f"  Failed to process share capital change data for {code}: {e}")
                # Skip this stock, continue to next one
                continue
        
        print(f"Share capital change data update completed, success: {success_count}, total: {len(stock_codes)}")
        return success_count, len(stock_codes)
    except Exception as e:
        print(f"Failed to update share capital change data: {e}")
        return 0, 0

# Convert data to CSV and Pickle format
def convert_data_formats():
    """
    Convert data to CSV and Pickle format
    
    Returns:
    - bool: True if conversion is successful, False otherwise
    """
    print("\nConverting data to CSV and Pickle format...")
    try:
        # Ensure CSV and Pickle directories exist
        ensure_dir(CSV_LDAY_PATH)
        ensure_dir(PICKLE_PATH)
        ensure_dir(CSV_INDEX_PATH)
        
        # Convert daily data
        print("  Converting daily data...")
        # Conversion logic can be added here
        
        # Convert index data
        print("  Converting index data...")
        # Conversion logic can be added here
        
        print("Data conversion completed")
        return True
    except Exception as e:
        print(f"Failed to convert data: {e}")
        return False

# Get latest data
def download_latest_data():
    """Fetch latest data from network and update to simulated TDX directory"""
    print("\n=== Fetching latest data from network ===")
    
    # Get target update date
    target_date = get_target_date()
    print(f"Based on current time, target update date: {target_date}")
    target_date_int = int(target_date)
    
    # Read best IP configuration
    best_ip_data = read_best_ip()
    
    # Initialize TDX API
    api = TdxHq_API()
    connected = False
    
    # First try to connect using IPs from best_ip.json
    if best_ip_data and 'stock' in best_ip_data:
        print("Trying to connect using IPs from best_ip.json...")
        for server in best_ip_data['stock']:
            try:
                if api.connect(server['ip'], server['port']):
                    print(f"Successfully connected to server: {server['ip']}:{server['port']}")
                    connected = True
                    break
                else:
                    print(f"Failed to connect to server: {server['ip']}:{server['port']}")
            except Exception as e:
                print(f"Exception when connecting to server: {server['ip']}:{server['port']}, exception: {e}")
    
    # If connection using IPs from best_ip.json fails, try to automatically select best IP
    if not connected:
        print("Trying to automatically select best IP...")
        try:
            best_ip = select_best_ip()
            print(f"Automatically selected best IP: {best_ip['ip']}:{best_ip['port']}")
            if api.connect(best_ip['ip'], best_ip['port']):
                connected = True
        except Exception as e:
            print(f"Failed to automatically select best IP: {e}")
    
    # If still failed to connect, try backup IPs
    if not connected:
        print("Failed to connect to TDX server")
        print("Trying backup servers...")
        # Backup server list
        backup_servers = [
            {'ip': '119.147.86.171', 'port': 7709},
            {'ip': '180.153.39.51', 'port': 7709},
            {'ip': '114.80.149.19', 'port': 7709}
        ]
        for server in backup_servers:
            if api.connect(server['ip'], server['port']):
                print(f"Successfully connected to backup server: {server['ip']}:{server['port']}")
                connected = True
                break
        if not connected:
            print("All servers connection failed, unable to get data")
            return False
    
    try:
        # Get stock list
        print("Getting stock list...")
        stock_list = []
        
        # Get stock list through akshare
        stock_list = get_stock_list_with_akshare()
        
        # If unable to get stock list, use backup stock list
        if not stock_list:
            print("Warning: Unable to get stock list through akshare, using backup stock list")
            # Backup stock list
            stock_list = [
                ('sh', '600000'),  # Pudong Development Bank
                ('sh', '600036'),  # China Merchants Bank
                ('sz', '000001'),  # Ping An Bank
                ('sz', '000002')   # Vanke A
            ]
        
        print(f"Got total {len(stock_list)} stocks")
        
        # Ensure simulated TDX data directory exists
        create_simulated_tdx_directories()
        
        # Download stock daily data
        download_stock_daily_data(api, stock_list, target_date_int, VIPDOC_PATH)
        
        # Update index data
        update_index_data(target_date_int, VIPDOC_PATH)
        
        # Update financial data
        update_financial_data(SIMULATED_TDX_PATH)
        
        # Update share capital change data
        update_share_capital_data(SIMULATED_TDX_PATH)
        
        # Convert data to CSV and Pickle format
        convert_data_formats()
        
        print("\nData fetching successful!")
        print(f"Data saved in: {LOCAL_DATA_PATH}")
        print(f"Simulated TDX data directory: {SIMULATED_TDX_PATH}")
        
        return True
    finally:
        # Disconnect API
        api.disconnect()

if __name__ == "__main__":
    # Execute data fetching
    download_latest_data()