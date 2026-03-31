#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
程序通用函数库
作者：wking [http://wkings.net]
"""

import os
import statistics
import time
import datetime
import requests
import numpy as np
import pandas as pd
import threading
from queue import Queue
from retry import retry
# from rich.progress import track
# from rich import print
from tqdm import tqdm
import user_config as ucfg

# 设置pandas选项以避免fillna/ffill/bfill的向下转换警告
pd.set_option('future.no_silent_downcasting', True)

# debug输出函数
def user_debug(print_str, print_value='', ):
    """第一个参数为变量名称，第二个参数为变量的值"""
    if ucfg.debug:
        if print_value:
            print(str(print_str) + ' = ' + str(print_value))
        else:
            print(str(print_str))


# 将通达信的日线文件转换成CSV格式保存函数。通达信数据文件32字节为一组。
def day2csv(source_dir, file_name, target_dir):
    """
    将通达信的日线文件转换成CSV格式保存函数。通达信数据文件32字节为一组
    :param source_dir: str 源文件路径
    :param file_name: str 文件名
    :param target_dir: str 要保存的路径
    :return: none
    """
    import os
    import sys
    import math
    from struct import unpack
    from decimal import Decimal  # 用于浮点数四舍五入
    from decimal import InvalidOperation

    # 以二进制方式打开源文件
    source_path = source_dir + os.sep + file_name  # 源文件包含文件名的路径
    source_file = open(source_path, 'rb')
    buf = source_file.read()  # 读取源文件保存在变量中
    source_file.close()
    source_size = os.path.getsize(source_path)  # 获取源文件大小
    source_row_number = int(source_size / 32)
    # user_debug('源文件行数', source_row_number)

    # 打开目标文件，后缀名为CSV
    target_path = target_dir + os.sep + file_name[2:-4] + '.csv'  # 目标文件包含文件名的路径
    # user_debug('target_path', target_path)

    if not os.path.isfile(target_path):
        # 目标文件不存在。写入表头行。begin从0开始转换
        target_file = open(target_path, 'w', encoding="utf-8")  # 以覆盖写模式打开文件
        header = str('date') + ',' + str('code') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
                 + str('close') + ',' + str('vol') + ',' + str('amount')
        target_file.write(header)
        begin = 0
        end = begin + 32
        row_number = 0
        last_date = None
    else:
        # 不为0，文件有内容。行附加。
        # 尝试读取文件，处理不同编码情况
        target_file_content = None
        row_number = 0
        last_date = None
        
        # 尝试不同编码读取文件
        encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
        for encoding in encodings:
            try:
                target_file = open(target_path, 'r+', encoding=encoding)  # 尝试不同编码
                target_file_content = target_file.readlines()
                target_file.close()
                break  # 成功读取后退出循环
            except UnicodeDecodeError:
                if 'target_file' in locals():
                    target_file.close()
                continue  # 尝试下一种编码
            except Exception as e:
                if 'target_file' in locals():
                    target_file.close()
                print(f"读取文件{target_path}时出错: {e}")
                break
        
        if target_file_content is None:
            # 所有编码都尝试失败，重新创建文件
            print(f"无法读取文件{target_path}，将重新创建")
            target_file = open(target_path, 'w', encoding="utf-8")  # 以覆盖写模式打开文件
            header = str('date') + ',' + str('code') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
                     + str('close') + ',' + str('vol') + ',' + str('amount')
            target_file.write(header)
            target_file.close()
            begin = 0
            row_number = 0
            last_date = None
        else:
            row_number = len(target_file_content)  # 获得文件行数
            
            if row_number <= 1:  # 只有表头行或空文件
                begin = 0
                last_date = None
            else:
                # 获取目标文件最后一行的日期
                last_line = target_file_content[-1].strip()
                if last_line:
                    last_date = last_line.split(',')[0]
                else:
                    # 如果最后一行是空行，继续往上找
                    for i in range(row_number-2, 0, -1):
                        line = target_file_content[i].strip()
                        if line:
                            last_date = line.split(',')[0]
                            break
                    else:
                        last_date = None
        
        # 确定在通达信.day文件中的起始位置
        begin = 0
        end = begin + 32
        found = False
        
        # 如果有最后日期，遍历通达信文件找到对应的位置
        if last_date:
            for i in range(source_row_number):
                # 读取当前32字节的数据
                record = buf[begin:end]
                if len(record) < 32:
                    break
                
                # 解析日期
                a = unpack('IIIIIfII', record)
                date_str = str(a[0]).zfill(8)
                try:
                    year = int(date_str[0:4])
                    month = int(date_str[4:6])
                    day = int(date_str[6:8])
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        current_date = f"{year}-{month:02d}-{day:02d}"
                        if current_date == last_date:
                            # 找到最后日期，从下一条记录开始
                            begin += 32
                            end += 32
                            found = True
                            break
                except (ValueError, IndexError):
                    pass
                
                begin += 32
                end += 32
        
        # 如果没找到最后日期，只处理最新的数据，而不是从开头重新开始
        # 这样可以避免产生重复数据
        if not found:
            # 如果没找到最后日期，说明可能是文件格式问题或日期不匹配
            # 只处理最新的几条记录，避免重复数据
            print(f"警告：在文件{file_path}中未找到最后日期{last_date}，将只处理最新记录")
            # 计算最新记录的位置，只处理最近100条记录
            recent_records = min(100, source_row_number)
            begin = (source_row_number - recent_records) * 32
            end = begin + 32
        
    # 收集所有数据行，以便按日期排序
    data_rows = []
    
    # 计算需要处理的记录数量
    remaining_records = source_row_number - (begin // 32)
    
    for _ in range(remaining_records):
        # 确保还有足够的字节可读
        if begin + 32 > len(buf):
            break
            
        # 将字节流转换成Python数据格式
        # I: unsigned int
        # f: float
        # a[5]浮点类型的成交金额，使用decimal类四舍五入为整数
        a = unpack('IIIIIfII', buf[begin:end])
        # a[0]  将'19910404'样式的整数转为'1991-04-04'格式的字符串。为了统一日期格式
        # 确保日期是8位字符串，不足前面补0
        date_str = str(a[0]).zfill(8)
        # 检查日期有效性：年份在合理范围内，月份1-12，日期1-31
        try:
            year = int(date_str[0:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])
            # 验证日期有效性
            if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                a_date = f"{year}-{month:02d}-{day:02d}"
            else:
                # 无效日期，跳过
                begin += 32
                end += 32
                continue
        except (ValueError, IndexError):
            # 无效日期格式，跳过
            begin += 32
            end += 32
            continue
        
        # 处理成交金额，确保是有效的浮点数
        amount = a[5]
        try:
            # 检查amount是否为有效浮点数
            if isinstance(amount, (int, float)) and not (math.isnan(amount) or math.isinf(amount)):
                # 四舍五入为整数
                amount_str = str(Decimal(str(amount)).quantize(Decimal("1."), rounding="ROUND_HALF_UP"))
            else:
                amount_str = '0'
        except (InvalidOperation, ValueError):
            # 处理无效操作，设置金额为0
            amount_str = '0'
        
        # 构建数据行，包含日期以便排序
        data_row = {
            'date': a_date,
            'code': file_name[2:-4],
            'open': str(a[1] / 100.0),
            'high': str(a[2] / 100.0),
            'low': str(a[3] / 100.0),
            'close': str(a[4] / 100.0),
            'vol': str(a[6]),
            'amount': amount_str
        }
        data_rows.append(data_row)
        
        begin += 32
        end += 32
    
    # 如果没有新数据，直接返回
    if not data_rows:
        print(f"文件{target_path}没有新数据需要追加")
        return
    
    # 按日期排序，确保数据顺序正确
    data_rows.sort(key=lambda x: x['date'])
    
    # 读取现有的CSV文件内容，以便去重
    if row_number > 0:
        # 文件已存在，读取现有内容
        existing_content = None
        encodings = ['utf-8', 'gbk', 'gb2312', 'latin-1']
        for encoding in encodings:
            try:
                with open(target_path, 'r', encoding=encoding) as f:
                    existing_content = f.readlines()
                break  # 成功读取后退出循环
            except UnicodeDecodeError:
                continue  # 尝试下一种编码
            except Exception as e:
                print(f"读取文件{target_path}时出错: {e}")
                continue  # 尝试下一种编码，而不是直接break
        
        if existing_content is None:
            # 所有编码都尝试失败，重新创建文件
            print(f"无法读取文件{target_path}，将重新创建")
            try:
                # 重新打开文件，写入排序后的数据
                with open(target_path, 'w', encoding="utf-8") as target_file:  # 以覆盖写模式打开文件
                    header = str('date') + ',' + str('code') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
                             + str('close') + ',' + str('vol') + ',' + str('amount')
                    target_file.write(header)
                    # 写入排序后的数据
                    for row in data_rows:
                        line = f"\n{row['date']},{row['code']},{row['open']},{row['high']},{row['low']},{row['close']},{row['vol']},{row['amount']}"
                        target_file.write(line)
                print(f"文件{target_path}已重新创建，写入了{len(data_rows)}条数据")
            except PermissionError as e:
                print(f"无法写入文件{target_path}：权限不足 - {e}")
            except Exception as e:
                print(f"创建文件{target_path}时出错：{e}")
            return
        
        if existing_content:
            # 保留表头
            header = existing_content[0].strip()
            # 获取已有的日期列表，用于去重
            existing_dates = set()
            for line in existing_content[1:]:
                if line.strip():
                    parts = line.strip().split(',')
                    if len(parts) >= 1:
                        existing_dates.add(parts[0])
            
            # 过滤掉已存在的数据行
            new_data_rows = []
            for row in data_rows:
                if row['date'] not in existing_dates:
                    new_data_rows.append(row)
            
            if not new_data_rows:
                print(f"文件{target_path}没有新数据需要追加")
                return
            
            # 重新打开文件，写入去重后的新数据
            try:
                with open(target_path, 'w', encoding="utf-8") as target_file:
                    target_file.write(header)
                    # 先写入已有的数据
                    for line in existing_content[1:]:
                        if line.strip():
                            target_file.write('\n' + line.strip())
                    # 再写入新数据
                    for row in new_data_rows:
                        line = f"\n{row['date']},{row['code']},{row['open']},{row['high']},{row['low']},{row['close']},{row['vol']},{row['amount']}"
                        target_file.write(line)
                print(f"文件{target_path}已更新，添加了{len(new_data_rows)}条新数据")
            except PermissionError as e:
                print(f"无法写入文件{target_path}：权限不足 - {e}")
            except Exception as e:
                print(f"更新文件{target_path}时出错：{e}")
    else:
        # 新建文件
        try:
            # 重新打开文件，写入排序后的数据
            with open(target_path, 'w', encoding="utf-8") as target_file:  # 以覆盖写模式打开文件
                header = str('date') + ',' + str('code') + ',' + str('open') + ',' + str('high') + ',' + str('low') + ',' \
                         + str('close') + ',' + str('vol') + ',' + str('amount')
                target_file.write(header)
                
                # 写入排序后的数据
                for row in data_rows:
                    line = f"\n{row['date']},{row['code']},{row['open']},{row['high']},{row['low']},{row['close']},{row['vol']},{row['amount']}"
                    target_file.write(line)
            print(f"文件{target_path}已创建，写入了{len(data_rows)}条数据")
        except PermissionError as e:
            print(f"无法写入文件{target_path}：权限不足 - {e}")
        except Exception as e:
            print(f"创建文件{target_path}时出错：{e}")


def get_TDX_blockfilecontent(filename):
    """
    读取本机通达信板块文件，获取文件内容
    :rtype: object
    :param filename: 字符串类型。输入的文件名。
    :return: DataFrame类型
    """
    from pytdx.reader import block_reader, TdxFileNotFoundException
    if ucfg.tdx['tdx_path']:
        filepath = ucfg.tdx['tdx_path'] + os.sep + 'T0002' + os.sep + 'hq_cache' + os.sep + filename
        df = block_reader.BlockReader().get_df(filepath)
    else:
        print("user_config文件的tdx_path变量未配置，或未找到" + filename + "文件")
    return df


def get_lastest_stocklist():
    """
    使用pytdx从网络获取最新券商列表
    :return:DF格式，股票清单
    """
    import pytdx.hq
    import pytdx.util.best_ip
    print(f"优选通达信行情服务器 也可直接更改为优选好的 {{'ip': '123.125.108.24', 'port': 7709}}")
    # ipinfo = pytdx.util.best_ip.select_best_ip()
    api = pytdx.hq.TdxHq_API()
    # with api.connect(ipinfo['ip'], ipinfo['port']):
    with api.connect('123.125.108.24', 7709):
        data = pd.concat([pd.concat(
            [api.to_df(api.get_security_list(j, i * 1000)).assign(sse='sz' if j == 0 else 'sh') for i in
             range(int(api.get_security_count(j) / 1000) + 1)], axis=0) for j in range(2)], axis=0)
    data = data.reindex(columns=['sse', 'code', 'name', 'pre_close', 'volunit', 'decimal_point'])
    data.sort_values(by=['sse', 'code'], ascending=True, inplace=True)
    data.reset_index(drop=True, inplace=True)
    # 这个方法不行 字符串不能运算大于小于，转成int更麻烦
    # df = data.loc[((data['sse'] == 'sh') & ((data['code'] >= '600000') | (data['code'] < '700000'))) | \
    #              ((data['sse'] == 'sz') & ((data['code'] >= '000001') | (data['code'] < '100000'))) | \
    #              ((data['sse'] == 'sz') & ((data['code'] >= '300000') | (data['code'] < '309999')))]
    sh_start_num = data[(data['sse'] == 'sh') & (data['code'] == '600000')].index.tolist()[0]
    sh_end_num = data[(data['sse'] == 'sh') & (data['code'] == '706070')].index.tolist()[0]
    sz00_start_num = data[(data['sse'] == 'sz') & (data['code'] == '000001')].index.tolist()[0]
    sz00_end_num = data[(data['sse'] == 'sz') & (data['code'] == '100303')].index.tolist()[0]
    sz30_start_num = data[(data['sse'] == 'sz') & (data['code'] == '300001')].index.tolist()[0]
    sz30_end_num = data[(data['sse'] == 'sz') & (data['code'] == '395001')].index.tolist()[0]

    df_sh = data.iloc[sh_start_num:sh_end_num]
    df_sz00 = data.iloc[sz00_start_num:sz00_end_num]
    df_sz30 = data.iloc[sz30_start_num:sz30_end_num]

    df = pd.concat([df_sh, df_sz00, df_sz30])
    df.reset_index(drop=True, inplace=True)
    return df


def historyfinancialreader(filepath):
    """
    读取解析通达信目录的历史财务数据
    :param filepath: 字符串类型。传入文件路径
    :return: DataFrame格式。返回解析出的财务文件内容
    """
    import struct

    cw_file = open(filepath, 'rb')
    header_pack_format = '<1hI1H3L'
    header_size = struct.calcsize(header_pack_format)
    stock_item_size = struct.calcsize("<6s1c1L")
    data_header = cw_file.read(header_size)
    stock_header = struct.unpack(header_pack_format, data_header)
    max_count = stock_header[2]
    report_date = stock_header[1]
    report_size = stock_header[4]
    report_fields_count = int(report_size / 4)
    report_pack_format = '<{}f'.format(report_fields_count)
    results = []
    for stock_idx in range(0, max_count):
        cw_file.seek(header_size + stock_idx * struct.calcsize("<6s1c1L"))
        si = cw_file.read(stock_item_size)
        stock_item = struct.unpack("<6s1c1L", si)
        code = stock_item[0].decode("utf-8")
        foa = stock_item[2]
        cw_file.seek(foa)
        info_data = cw_file.read(struct.calcsize(report_pack_format))
        data_size = len(info_data)
        cw_info = list(struct.unpack(report_pack_format, info_data))
        cw_info.insert(0, code)
        results.append(cw_info)
    cw_file.close()
    df = pd.DataFrame(results)
    return df


class ManyThreadDownload:
    def __init__(self, num=10):
        self.num = num  # 线程数,默认10
        self.url = ''  # url
        self.name = ''  # 目标地址
        self.total = 0  # 文件大小

    # 获取每个线程下载的区间
    def get_range(self):
        ranges = []
        offset = int(self.total / self.num)
        for i in range(self.num):
            if i == self.num - 1:
                ranges.append((i * offset, ''))
            else:
                ranges.append(((i * offset), (i + 1) * offset - 1))
        return ranges  # [(0,99),(100,199),(200,"")]

    # 通过传入开始和结束位置来下载文件
    def download(self, ts_queue):
        while not ts_queue.empty():
            start_, end_ = ts_queue.get()
            headers = {
                'Range': 'Bytes=%s-%s' % (start_, end_),
                'Accept-Encoding': '*'
            }
            flag = False
            while not flag:
                try:
                    # 设置重连次数
                    requests.adapters.DEFAULT_RETRIES = 10
                    # s = requests.session()            # 每次都会发起一次TCP握手,性能降低，还可能因发起多个连接而被拒绝
                    # # 设置连接活跃状态为False
                    # s.keep_alive = False
                    # 默认stream=false,立即下载放到内存,文件过大会内存不足,大文件时用True需改一下码子
                    res = requests.get(self.url, headers=headers)
                    res.close()  # 关闭请求  释放内存
                except Exception as e:
                    print((start_, end_, "出错了,连接重试:%s", e,))
                    time.sleep(1)
                    continue
                flag = True

            # print("\n", ("%s-%s download success" % (start_, end_)), end="", flush=True)
            # with lock:
            with open(self.name, "rb+") as fd:
                fd.seek(start_)
                fd.write(res.content)
            # self.fd.seek(start_)                                        # 指定写文件的位置,下载的内容放到正确的位置处
            # self.fd.write(res.content)                                  # 将下载文件保存到 fd所打开的文件里

    def run(self, url, name):
        self.url = url
        self.name = name
        self.total = int(requests.head(url).headers['Content-Length'])
        # file_size = int(urlopen(self.url).info().get('Content-Length', -1))
        file_size = self.total
        if os.path.exists(name):
            first_byte = os.path.getsize(name)
        else:
            first_byte = 0
        if first_byte >= file_size:
            return file_size

        self.fd = open(name, "wb")  # 续传时直接rb+ 文件不存在时会报错,先wb再rb+
        self.fd.truncate(self.total)  # 建一个和下载文件一样大的文件,不是必须的,stream=True时会用到
        self.fd.close()
        # self.fd = open(self.name, "rb+")           # 续传时ab方式打开时会强制指针指向文件末尾,seek并不管用,应用rb+模式
        thread_list = []
        ts_queue = Queue()  # 用队列的线程安全特性，以列表的形式把开始和结束加到队列
        for ran in self.get_range():
            start_, end_ = ran
            ts_queue.put((start_, end_))

        for i in range(self.num):
            t = threading.Thread(target=self.download, name='th-' + str(i), kwargs={'ts_queue': ts_queue})
            t.setDaemon(True)
            thread_list.append(t)
        for t in thread_list:
            t.start()
        for t in thread_list:
            t.join()  # 设置等待，全部线程完事后再继续

        self.fd.close()


@retry(tries=3, delay=3)  # 无限重试装饰性函数
def dowload_url(url):
    """
    :param url:要下载的url
    :return: request.get实例化对象
    """
    import requests
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/87.0.4280.141',
    }
    response_obj = requests.get(url, headers=header, timeout=5)  # get方式请求
    response_obj.raise_for_status()  # 检测异常方法。如有异常则抛出，触发retry
    # print(f'{url} 下载完成')
    return response_obj


def list_localTDX_cwfile(ext_name):
    """
    列出本地已有的专业财务文件。返回文件列表
    :param ext_name: str类型。文件扩展名。返回指定扩展名的文件列表
    :return: list类型。财务专业文件列表
    """
    cw_path = ucfg.tdx['tdx_path'] + os.sep + "vipdoc" + os.sep + "cw"
    tmplist = os.listdir(cw_path)  # 遍历通达信vipdoc/cw目录
    cw_filelist = []
    for file in tmplist:  # 只保留gpcw????????.扩展名 格式文件
        # 修复：文件名长度应该是15或16字符（gpcw + 8位日期 + .扩展名）
        # gpcw20071231.zip 长度是15字符
        if (len(file) == 15 or len(file) == 16) and file[:4] == "gpcw" and file.endswith("." + ext_name):
            cw_filelist.append(file)
    # print(f'检测到{len(cw_filelist)}个专业财务文件')
    return cw_filelist


def readall_local_cwfile():
    """
    将全部财报文件读到df_cw字典里。会占用1G内存，但处理速度比遍历CSV方式快很多
    :return: 字典形式，所有财报内容。
    """
    print(f'开始载入所有财报文件到内存')
    dict = {}
    cwfile_list = os.listdir(ucfg.tdx['csv_cw'])  # cw目录 生成文件名列表
    starttime_tick = time.time()
    for cwfile in cwfile_list:
        # 只处理.pkl文件，跳过.dat和.zip文件
        if cwfile.endswith('.pkl') and os.path.getsize(ucfg.tdx['csv_cw'] + os.sep + cwfile) != 0:
            dict[cwfile[4:-4]] = pd.read_pickle(ucfg.tdx['csv_cw'] + os.sep + cwfile, compression=None)
    print(f'读取所有财报文件完成 用时{(time.time() - starttime_tick):.2f}秒')
    return dict


def make_fq(code, df_code, df_gbbq, df_cw='', start_date='', end_date='', fqtype='qfq'):
    """
    股票周期数据复权处理函数
    :param code:str格式，具体股票代码
    :param df_code:DF格式，未除权的具体股票日线数据。DF自动生成的数字索引，列定义：date,open,high,low,close,vol,amount
    :param df_gbbq:DF格式，通达信导出的全股票全日期股本变迁数据。DF读取gbbq文件必须加入dtype={'code': str}参数，否则股票代码开头0会忽略
    :param df_cw:DF格式，读入内存的全部财务文件
    :param start_date:可选，要截取的起始日期。默认为空。格式"2020-10-10"
    :param end_date:可选，要截取的截止日期。默认为空。格式"2020-10-10"
    :param fqtype:可选，复权类型。默认前复权。
    :return:复权后的DF格式股票日线数据
    """

    '''以下是从https://github.com/rainx/pytdx/issues/78#issuecomment-335668322 提取学习的前复权代码
    import datetime

    import numpy as np
    import pandas as pd
    from pytdx.hq import TdxHq_API
    # from pypinyin import lazy_pinyin
    import tushare as ts

    '除权除息'
    api = TdxHq_API()

    with api.connect('180.153.39.51', 7709):
        # 从服务器获取该股的股本变迁数据
        category = {
            '1': '除权除息', '2': '送配股上市', '3': '非流通股上市', '4': '未知股本变动', '5': '股本变化',
            '6': '增发新股', '7': '股份回购', '8': '增发新股上市', '9': '转配股上市', '10': '可转债上市',
            '11': '扩缩股', '12': '非流通股缩股', '13': '送认购权证', '14': '送认沽权证'}
        data = api.to_df(api.get_xdxr_info(0, '000001'))
        data = data \
            .assign(date=pd.to_datetime(data[['year', 'month', 'day']])) \
            .drop(['year', 'month', 'day'], axis=1) \
            .assign(category_meaning=data['category'].apply(lambda x: category[str(x)])) \
            .assign(code=str('000001')) \
            .rename(index=str, columns={'panhouliutong': 'liquidity_after',
                                        'panqianliutong': 'liquidity_before', 'houzongguben': 'shares_after',
                                        'qianzongguben': 'shares_before'}) \
            .set_index('date', drop=False, inplace=False)
        xdxr_data = data.assign(date=data['date'].apply(lambda x: str(x)[0:10]))  # 该股的股本变迁DF处理完成
        df_gbbq = xdxr_data[xdxr_data['category'] == 1]  # 提取只有除权除息的行保存到DF df_gbbq
        # print(df_gbbq)

        # 从服务器读取该股的全部历史不复权K线数据，保存到data表，  只包括 日期、开高低收、成交量、成交金额数据
        data = pd.concat([api.to_df(api.get_security_bars(9, 0, '000001', (9 - i) * 800, 800)) for i in range(10)], axis=0)

        # 从data表加工数据，保存到bfq_data表
        df_code = data \
            .assign(date=pd.to_datetime(data['datetime'].apply(lambda x: x[0:10]))) \
            .assign(code=str('000001')) \
            .set_index('date', drop=False, inplace=False) \
            .drop(['year', 'month', 'day', 'hour',
                   'minute', 'datetime'], axis=1)
        df_code['if_trade'] = True
        # 不复权K线数据处理完成，保存到bfq_data表

        # 提取info表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是除权除息日的行
        data = pd.concat([df_code, df_gbbq[['category']][df_code.index[0]:]], axis=1)
        # print(data)

        data['date'] = data.index
        # 修复pandas新版本的fillna方法使用
        data['if_trade'] = data['if_trade'].fillna(value=False).infer_objects(copy=False)  # if_trade列，无效的值填充为False
        data = data.ffill().infer_objects(copy=False)  # 向下填充无效值

        # 提取info表的'fenhong', 'peigu', 'peigujia',‘songzhuangu'列的值，按日期一一对应，列拼接到data表。
        # 也就是将当日是除权除息日的行，对应的除权除息数据，写入对应的data表的行。
        data = pd.concat([data, df_gbbq[['fenhong', 'peigu', 'peigujia',
                                      'songzhuangu']][df_code.index[0]:]], axis=1)
        data = data.fillna(0).infer_objects(copy=False)  # 无效值填空0

        data['preclose'] = (data['close'].shift(1) * 10 - data['fenhong'] + data['peigu']
                            * data['peigujia']) / (10 + data['peigu'] + data['songzhuangu'])
        data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1).infer_objects(copy=False)[::-1].cumprod()  # 计算每日复权因子
        data['open'] = data['open'] * data['adj']
        data['high'] = data['high'] * data['adj']
        data['low'] = data['low'] * data['adj']
        data['close'] = data['close'] * data['adj']
        data['preclose'] = data['preclose'] * data['adj']

        data = data[data['if_trade']]
        result = data \
            .drop(['fenhong', 'peigu', 'peigujia', 'songzhuangu', 'if_trade', 'category'], axis=1)[data['open'] != 0] \
            .assign(date=data['date'].apply(lambda x: str(x)[0:10]))
        print(result)
    '''

    # 先进行判断。如果有adj列，且没有NaN值，表示此股票数据已处理完成，无需处理。直接返回原数据。
    # 如果没有‘adj'列，表示没进行过复权处理，当作新股处理
    if 'adj' in df_code.columns.to_list():
        if True in df_code['adj'].isna().to_list():
            first_index = np.where(df_code.isna())[0][0]  # 有NaN值，设为第一个NaN值所在的行
        else:
            # 返回原数据，而不是空字符串，这样pkl文件会被更新
            return df_code
    else:
        first_index = 0
        flag_newstock = True

    flag_attach = False  # True=追加数据模式  False=数据全部重新计算
    # 设置新股标志。True=新股，False=旧股。新股跳过追加数据部分的代码。如果没定义，默认为False
    if 'flag_newstock' not in dir():
        flag_newstock = False

    # 提取该股除权除息行保存到DF df_cqcx，提取其他信息行到df_gbbq
    df_cqcx = df_gbbq.loc[(df_gbbq['code'] == code) & (df_gbbq['类别'] == '除权除息')]
    df_gbbq = df_gbbq.loc[(df_gbbq['code'] == code) & (
            (df_gbbq['类别'] == '股本变化') |
            (df_gbbq['类别'] == '送配股上市') |
            (df_gbbq['类别'] == '转配股上市'))]

    # 清洗df_gbbq，可能出现同一日期有 配股上市、股本变化两行数据。不清洗后面合并会索引冲突。
    # 下面的代码可以保证删除多个不连续的重复行，用DF dropdup方法不能确保删除的值是大是小
    # 如果Ture在列表里。表示有重复行存在
    if True in df_gbbq.duplicated(subset=['权息日'], keep=False).to_list():
        #  提取重复行的索引
        del_index = []  # 要删除的后流通股的值
        tmp_dict = df_gbbq.duplicated(subset=['权息日'], keep=False).to_dict()
        for k, v in tmp_dict.items():
            if v:
                del_index.append(df_gbbq.at[k, '送转股-后流通盘'])
                # 如果dup_index有1个以上的值，且K+1的元素是False，或K+1不存在也返回False，表示下一个元素 不是 重复行
                if len(del_index) > 1 and (tmp_dict.get(k + 1, False) == False):
                    del_index.remove(max(del_index))  # 删除最大值
                    # 选择剩余的值，取反，则相当于保留了最大值，删除了其余的值
                    df_gbbq = df_gbbq[~df_gbbq['送转股-后流通盘'].isin(del_index)]

    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04 为了按日期一一对应拼接
    df_cqcx = df_cqcx.assign(date=pd.to_datetime(df_cqcx['权息日'], format='%Y%m%d'))  # 添加date列，设置为datetime64[ns]格式
    df_cqcx.set_index('date', drop=True, inplace=True)  # 设置权息日为索引  (字符串表示的日期 "19910101")
    df_cqcx['category'] = 1.0  # 添加category列
    df_gbbq = df_gbbq.assign(date=pd.to_datetime(df_gbbq['权息日'], format='%Y%m%d'))  # 添加date列，设置为datetime64[ns]格式
    df_gbbq.set_index('date', drop=True, inplace=True)  # 设置权息日为索引  (字符串表示的日期 "19910101")
    if len(df_cqcx) > 0:  # =0表示股本变迁中没有该股的除权除息信息。gbbq_lastest_date设置为今天，当作新股处理
        cqcx_lastest_date = df_cqcx.index[-1].strftime('%Y-%m-%d')  # 提取最新的除权除息日
    else:
        cqcx_lastest_date = str(datetime.date.today())
        flag_newstock = True

    # 判断df_code是否已有历史数据，是追加数据还是重新生成。
    # 如果gbbq_lastest_date not in df_code.loc[first_index:, 'date'].to_list()，表示未更新数据中不包括除权除息日
    # 由于前复权的特性，除权后历史数据都要变。因此未更新数据中不包括除权除息日，只需要计算未更新数据。否则日线数据需要全部重新计算
    # 如果'adj'在df_code的列名单里，表示df_code是已复权过的，只需追加新数据，否则日线数据还是需要全部重新计算
    if cqcx_lastest_date not in df_code.loc[first_index:, 'date'].to_list() and not flag_newstock:
        if 'adj' in df_code.columns.to_list():
            flag_attach = True  # 确定为追加模式
            df_code_original = df_code  # 原始code备份为df_code_original，最后合并
            df_code = df_code.iloc[first_index:]  # 切片df_code，只保留需要处理的行
            df_code.reset_index(drop=True, inplace=True)
            df_code_original.dropna(how='any', inplace=True)  # 丢掉缺失数据的行，之后直接append新数据就行。比merge简单。
            df_code_original['date'] = pd.to_datetime(df_code_original['date'], format='%Y-%m-%d')  # 转为时间格式
            df_code_original.set_index('date', drop=True, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并

    # 单独提取流通股处理。因为流通股是设置流通股变更时间节点，最后才填充nan值。和其他列的处理会冲突。
    # 如果有流通股列，单独复制出来；如果没有流通股列，添加流通股列，赋值为NaN。
    # 如果是追加数据模式，则肯定已存在流通股列且数据已处理。因此不需单独提取流通股列。只在前复权前处理缺失的流通股数据即可
    # 虽然财报中可能没有流通股的数据，但股本变迁文件中最少也有股票第一天上市时的流通股数据。
    # 且后面还会因为送配股上市、股本变化，导致在非财报日之前，流通股就发生变动
    if not flag_attach:
        if '流通股' in df_code.columns.to_list():
            df_ltg = pd.DataFrame(index=df_code.index)
            df_ltg['date'] = df_code['date']
            df_ltg['流通股'] = df_code['流通股']
            del df_code['流通股']
        else:
            df_ltg = pd.DataFrame(index=df_code.index)
            df_ltg['date'] = df_code['date']
            df_ltg['流通股'] = np.nan
    else:
        # 附加模式，此处df_code是已经切片过的，只包括需要更新的数据行。其中也包含流通股列，值全为NaN。
        # 类似单独提出处理流通股列，和新股模式的区别是只处理需要更新的数据行。
        df_ltg = pd.DataFrame(index=df_code.index)
        del df_code['流通股']
        # 第一个值赋值为df_code_original流通股列第一个NaN值的前一个有效值
        ltg_lastest_value = df_code_original.at[df_code_original.index[-1], '流通股']
        df_ltg['date'] = df_code['date']
        df_ltg['流通股'] = np.nan
        df_ltg.at[0, '流通股'] = ltg_lastest_value
    df_gbbq = df_gbbq.rename(columns={'送转股-后流通盘': '流通股'})  # 列改名，为了update可以匹配
    # 用df_gbbq update data，由于只有流通股列重复，因此只会更新流通股列对应索引的NaN值
    df_ltg['date'] = pd.to_datetime(df_ltg['date'], format='%Y-%m-%d')  # 转为时间格式
    df_ltg.set_index('date', drop=True, inplace=True)  # 时间为索引。方便与另外复权的DF表对齐合并
    df_ltg.update(df_gbbq, overwrite=False)  # 使用update方法更新df_ltg
    if not flag_attach:  # 附加模式则单位已经调整过，无需再调整
        # 股本变迁里的流通股单位是万股。转换与财报的单位：股 统一
        df_ltg['流通股'] = df_ltg['流通股'] * 10000

    # int64类型储存的日期19910404，转换为dtype: datetime64[ns] 1991-04-04  为了按日期一一对应拼接
    with pd.option_context('mode.chained_assignment', None):  # 临时屏蔽语句警告
        df_code['date'] = pd.to_datetime(df_code['date'], format='%Y-%m-%d')
    
    # 检查并移除重复的日期，确保索引唯一
    if df_code['date'].duplicated().any():
        print(f"股票{code}存在重复日期，将去重")
        df_code = df_code.drop_duplicates(subset='date', keep='last')
    
    df_code.set_index('date', drop=True, inplace=True)
    df_code.insert(df_code.shape[1], 'if_trade', True)  # 插入if_trade列，赋值True

    # 提取df_cqcx和df_gbbq表的category列的值，按日期一一对应，列拼接到bfq_data表。也就是标识出当日是股本变迁的行
    try:
        data = pd.concat([df_code, df_cqcx[['category']][df_code.index[0]:]], axis=1)
    except Exception as e:
        print(f"股票{code}在合并数据时出错：{e}")
        # 如果合并失败，返回原始数据
        return df_code
    # print(data)

    # 修复pandas新版本的fillna方法使用
    data['if_trade'] = data['if_trade'].fillna(value=False).infer_objects(copy=False)  # if_trade列，无效的值填充为False
    data = data.ffill().infer_objects(copy=False)  # 向下填充无效值 (替代fillna(method='ffill'))

    # 提取info表的'fenhong', 'peigu', 'peigujia',‘songzhuangu'列的值，按日期一一对应，列拼接到data表。
    # 也就是将当日是除权除息日的行，对应的除权除息数据，写入对应的data表的行。
    data = pd.concat([data, df_cqcx[['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                                     '送转股-后流通盘']][df_code.index[0]:]], axis=1)
    data = data.fillna(0).infer_objects(copy=False)  # 无效值填空0
    data['preclose'] = (data['close'].shift(1) * 10 - data['分红-前流通盘'] + data['配股-后总股本']
                        * data['配股价-前总股本']) / (10 + data['配股-后总股本'] + data['送转股-后流通盘'])
    # 计算每日复权因子 前复权最近一次股本变迁的复权因子为1
    data['adj'] = (data['preclose'].shift(-1) / data['close']).fillna(1).infer_objects(copy=False)[::-1].cumprod()
    data['open'] = data['open'] * data['adj']
    data['high'] = data['high'] * data['adj']
    data['low'] = data['low'] * data['adj']
    data['close'] = data['close'] * data['adj']
    # data['preclose'] = data['preclose'] * data['adj']  # 这行没用了
    data = data[data['if_trade']]  # 重建整个表，只保存if_trade列=true的行

    # 抛弃过程处理行，且open值不等于0的行
    data = data.drop(['分红-前流通盘', '配股-后总股本', '配股价-前总股本',
                      '送转股-后流通盘', 'if_trade', 'category', 'preclose'], axis=1)[data['open'] != 0]
    # 复权处理完成

    # 如果没有传参进来，就自己读取财务文件，否则用传参的值
    if df_cw == '':
        cw_dict = readall_local_cwfile()
    else:
        cw_dict = df_cw

    # 计算换手率
    # 财报数据公开后，股本才变更。因此有效时间是“当前财报日至未来日期”。故将结束日期设置为2099年。每次财报更新后更新对应的日期时间段
    e_date = '20990101'
    for cw_date in cw_dict:  # 遍历财报字典  cw_date=财报日期  cw_dict[cw_date]=具体的财报内容
        # 如果复权数据表的首行日期>当前要读取的财务报表日期，则表示此财务报表发布时股票还未上市，跳过此次循环。有例外情况：003001
        # (cw_dict[cw_date][0] == code).any() 表示当前股票code在财务DF里有数据
        if df_ltg.index[0].strftime('%Y%m%d') <= cw_date <= df_ltg.index[-1].strftime('%Y%m%d') \
                and len(cw_dict[cw_date]) > 0:
            if (cw_dict[cw_date][0] == code).any():
                # 获取目前股票所在行的索引值，具有唯一性，所以直接[0]
                code_df_index = cw_dict[cw_date][cw_dict[cw_date][0] == code].index.to_list()[0]
                # DF格式读取的财报，字段与财务说明文件的序号一一对应，如果是CSV读取的，字段需+1
                # print(f'{cw_date} 总股本:{cw_dict[cw_date].iat[code_df_index,238]}'
                #  f'流通股本:{cw_dict[cw_date].iat[code_df_index,239]}')
                # 如果流通股值是0，则进行下一次循环
                if int(cw_dict[cw_date].iat[code_df_index, 239]) != 0:
                    #  df_ltg[cw_date:e_date].index[0] 表示df_ltg中从cw_date到e_date的第一个索引的值。
                    #  也就是离cw_date日期最近的下一个有效行
                    df_ltg.at[df_ltg[cw_date:e_date].index[0], '流通股'] = float(cw_dict[cw_date].iat[code_df_index, 239])

    # df_ltg拼接回原DF
    data = pd.concat([data, df_ltg], axis=1)

    # 修复pandas新版本的fillna方法使用
    data = data.ffill().infer_objects(copy=False)  # 向下填充无效值 (替代fillna(method='ffill'))
    data = data.bfill().infer_objects(copy=False)  # 向上填充无效值  为了弥补开始几行的空值 (替代fillna(method='bfill'))
    data = data.round({'open': 2, 'high': 2, 'low': 2, 'close': 2, })  # 指定列四舍五入
    if '流通股' in data.columns.to_list():
        data['流通市值'] = data['流通股'] * data['close']
        data['换手率'] = (data['vol'] * 100) / data['流通股'] * 100  # vol单位是手，乘以100转换为股
        data = data.round({'流通市值': 2, '换手率': 2, })  # 指定列四舍五入
    if flag_attach:  # 追加模式，则附加最新处理的数据
        data = df_code_original.append(data)

    if len(start_date) == 0 and len(end_date) == 0:
        pass
    elif len(start_date) != 0 and len(end_date) == 0:
        data = data[start_date:]
    elif len(start_date) == 0 and len(end_date) != 0:
        data = data[:end_date]
    elif len(start_date) != 0 and len(end_date) != 0:
        data = data[start_date:end_date]
    data.reset_index(drop=False, inplace=True)  # 重置索引行，数字索引，date列到第1列，保存为str '1991-01-01' 格式
    # 最后调整列顺序
    # data = data.reindex(columns=['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount', 'adj', '流通股', '流通市值', '换手率'])
    return data


# 使用文件锁和本地文件来共享最优服务器信息，解决多进程缓存不一致问题
import threading
_cached_best_ip = {}  # 缓存最优服务器信息
_best_ip_lock = threading.Lock()


def custom_select_best_ip(_type='stock'):
    """
    自定义的最优服务器选择函数，实现一边寻找最优服务器一边删除bad response的服务器
    :param _type: str类型，'stock'或'future'，默认为'stock'
    :return: dict类型，包含'ip'和'port'的最优服务器信息
    """
    import datetime
    import os
    import json
    from pytdx.hq import TdxHq_API
    from pytdx.exhq import TdxExHq_API
    from pytdx.util.best_ip import stock_ip, future_ip
    
    # 本地服务器列表文件路径
    BEST_IP_FILE = os.path.join(os.path.dirname(__file__), 'best_ip.json')
    
    def ping(ip, port=7709, type_='stock'):
        """复制pytdx.util.best_ip中的ping函数"""
        api = TdxHq_API()
        apix = TdxExHq_API()
        __time1 = datetime.datetime.now()
        try:
            if type_ in ['stock']:
                connected = api.connect(ip, port, time_out=0.7)
                if connected:
                    try:
                        res = api.get_security_list(0, 1)
                        if res is not None:
                            if len(res) > 800:
                                print('GOOD RESPONSE {}'.format(ip))
                                return datetime.datetime.now() - __time1
                            else:
                                print('BAD RESPONSE {}'.format(ip))
                                return datetime.timedelta(9, 9, 0)
                        else:
                            print('BAD RESPONSE {}'.format(ip))
                            return datetime.timedelta(9, 9, 0)
                    finally:
                        api.disconnect()
                else:
                    return datetime.timedelta(9, 9, 0)
            elif type_ in ['future']:
                connected = apix.connect(ip, port, time_out=0.7)
                if connected:
                    try:
                        res = apix.get_instrument_count()
                        if res is not None:
                            if res > 20000:
                                print('GOOD RESPONSE {}'.format(ip))
                                return datetime.datetime.now() - __time1
                            else:
                                print('️Bad FUTUREIP REPSONSE {}'.format(ip))
                                return datetime.timedelta(9, 9, 0)
                        else:
                            print('️Bad FUTUREIP REPSONSE {}'.format(ip))
                            return datetime.timedelta(9, 9, 0)
                    finally:
                        apix.disconnect()
                else:
                    return datetime.timedelta(9, 9, 0)
        except Exception as e:
            if isinstance(e, TypeError):
                print(e)
                print('Tushare内置的pytdx版本和最新的pytdx 版本不同, 请重新安装pytdx以解决此问题')
                print('pip uninstall pytdx')
                print('pip install pytdx')
            else:
                print('BAD RESPONSE {}'.format(ip))
            return datetime.timedelta(9, 9, 0)
    
    def load_best_ip_from_file(_type):
        """从本地文件加载优化后的服务器列表"""
        try:
            if os.path.exists(BEST_IP_FILE):
                with open(BEST_IP_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    print(f"✅ 从本地文件加载了优化后的{_type}服务器列表")
                    return data.get(_type, [])
        except Exception as e:
            print(f"❌ 从本地文件加载服务器列表失败: {e}")
        return []
    
    def save_best_ip_to_file(stock_list, future_list):
        """将优化后的服务器列表保存到本地文件"""
        try:
            data = {
                'stock': stock_list,
                'future': future_list
            }
            with open(BEST_IP_FILE, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            print(f"✅ 优化后的服务器列表已保存到本地文件: {BEST_IP_FILE}")
        except Exception as e:
            print(f"❌ 保存服务器列表到本地文件失败: {e}")
    
    # 尝试从本地文件加载优化后的服务器列表
    local_ip_list = load_best_ip_from_file(_type)
    
    # 如果本地列表为空，使用pytdx库提供的默认列表
    if not local_ip_list:
        print(f"使用pytdx库提供的默认{_type}服务器列表")
        ip_list = stock_ip if _type == 'stock' else future_ip
    else:
        # 使用本地加载的优化列表
        ip_list = local_ip_list
    
    # 最多测试前10个服务器，避免耗时太长
    max_test_servers = 10
    ip_list_copy = ip_list[:max_test_servers].copy()
    
    results = []
    
    # 遍历服务器，测试但不删除bad response的服务器
    for ip_info in ip_list_copy:
        try:
            ping_time = ping(ip_info['ip'], ip_info['port'], _type)
            
            if ping_time < datetime.timedelta(0, 9, 0):
                # 服务器响应良好，添加到结果列表
                results.append((ping_time, ip_info))
                # 找到一个可用服务器就返回，不需要测试所有
                if len(results) >= 3:  # 找到3个可用服务器就足够了
                    break
            else:
                # 服务器响应不佳，只记录不删除
                print(f"bad response的服务器: {ip_info['ip']}:{ip_info['port']} (已记录，不删除)")
        except Exception as e:
            print(f"测试服务器{ip_info['ip']}:{ip_info['port']}时出错: {e}")
    
    # 快速保存服务器列表（简化版，不重新加载另一种类型）
    try:
        # 只保存当前类型的服务器列表，不处理另一种类型
        with open(BEST_IP_FILE, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    except Exception as e:
        existing_data = {
            'stock': [],
            'future': []
        }
    
    # 更新当前类型的服务器列表
    existing_data[_type] = ip_list
    
    # 保存到文件
    try:
        with open(BEST_IP_FILE, 'w', encoding='utf-8') as f:
            json.dump(existing_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        # 忽略保存失败，不影响主流程
        pass
    
    # 按照ping值从小大大排序
    if results:
        results = [x[1] for x in sorted(results, key=lambda x: x[0])]
        best_ip = results[0]
    else:
        # 如果所有测试的服务器都失败了，返回一个默认值
        default_ips = {
            'stock': {'ip': '123.125.108.24', 'port': 7709},
            'future': {'ip': '119.147.86.171', 'port': 7727}
        }
        best_ip = default_ips.get(_type, {'ip': '123.125.108.24', 'port': 7709})
    
    # 将最优服务器保存到缓存中
    _cached_best_ip[_type] = best_ip
    return best_ip


def get_tdx_lastestquote(stocklist=None):
    """
    使用pytdx获取当前实时行情。返回行情的DF列表格式。stocklist为空则获取ucfg.tdx['csv_lday']目录全部股票行情
    :param stocklist:  可选，list类型或str类型或tuple类型。传入股票列表['000001', '000002','600030']或单个股票代码'000001'或元组(1, '600030')
    :return:当前从pytdx服务器获取的最新股票行情
    """
    # get_security_quotes只允许最大80个股票为一组 数字越大漏掉的股票越多。测试数据：
    # 数字    获取股票    用时
    # 80    3554    2.59
    # 40    3874    5.07
    # 20    4015    10.12
    # 10    4105    17.54
    try:
        from pytdx.hq import TdxHq_API
        # 使用自定义的最优服务器选择函数替代原函数
    except ImportError as e:
        print(f'pytdx模块导入失败: {e}')
        return pd.DataFrame()

    stocklist_pytdx = []

    if stocklist is None:  # 如果列表为空，则获取csv_lday目录全部股票
        stocklist = []
        for i in os.listdir(ucfg.tdx['csv_lday']):
            stocklist.append(i[:-4])
    elif isinstance(stocklist, str):
        # 处理单个股票代码字符串
        stock_code = stocklist
        if stock_code.startswith('6'):
            stocklist_pytdx.append((1, stock_code))
        elif stock_code.startswith(('0', '3')):
            stocklist_pytdx.append((0, stock_code))
    elif isinstance(stocklist, tuple):
        # 处理元组格式 (市场代码, 股票代码)
        stocklist_pytdx.append(stocklist)
    elif isinstance(stocklist, list):
        # 处理股票代码列表
        for stock in stocklist:
            if isinstance(stock, tuple):
                # 如果列表中的元素是元组，直接添加
                stocklist_pytdx.append(stock)
            elif isinstance(stock, str):
                # 如果列表中的元素是字符串，转换为元组
                stock_code = stock
                if stock_code.startswith('6'):
                    stocklist_pytdx.append((1, stock_code))
                elif stock_code.startswith(('0', '3')):
                    stocklist_pytdx.append((0, stock_code))

    df = pd.DataFrame()
    starttime_tick = time.time()
    print(f'请求 {len(stocklist_pytdx)} 只股票实时行情')
    
    # 自动选择最优服务器
    best_ip = '123.125.108.24'  # 默认备用服务器
    best_port = 7709  # 默认备用端口
    
    try:
        print("正在测试最优服务器...")
        best_ip_info = custom_select_best_ip()
        best_ip = best_ip_info['ip']
        best_port = best_ip_info['port']
        print(f"✅ 找到最优服务器: {best_ip}:{best_port}")
    except Exception as e:
        print(f"❌ 自动选择最优服务器失败，使用默认服务器: {e}")
    
    # 尝试连接服务器并获取数据
    api = TdxHq_API()
    connected = False
    
    # 尝试连接多次
    for attempt in range(3):
        try:
            connected = api.connect(best_ip, best_port)
            if connected:
                print(f"✅ 服务器连接成功! (尝试 {attempt+1}/3)")
                
                # 分批获取股票行情，增加批次大小减少请求次数
                batch_size = 30
                for i in range(0, len(stocklist_pytdx), batch_size):
                    batch = stocklist_pytdx[i:i+batch_size]
                    try:
                        data = api.to_df(api.get_security_quotes(batch))
                        df = pd.concat([df, data], axis=0, ignore_index=True)
                    except Exception as e:
                        print(f'获取批次 {i//batch_size + 1} 失败: {e}')
                
                df.dropna(how='all', inplace=True)
                break  # 连接成功，跳出循环
            else:
                print(f"❌ 服务器连接失败 (尝试 {attempt+1}/3): 无法建立连接")
        except Exception as e:
            print(f"❌ 服务器连接失败 (尝试 {attempt+1}/3): {e}")
        finally:
            # 确保每次连接后都断开
            if connected:
                api.disconnect()
        
        if attempt < 2:  # 不是最后一次尝试，休眠1秒后重试
            time.sleep(1)
    
    if not connected:
        print("❌ 服务器连接失败，无法获取实时行情数据")
    
    print(f'已获取{len(df)}只股票行情 用时{(time.time() - starttime_tick):>5.2f}秒')
    
    # 只在有数据时打印列名
    if not df.empty:
        print(df.columns)
    
    # 筛选掉无效数据
    if not df.empty:
        # 只保留有有效数据的行
        df = df[df['code'].notna()]
        # 重置索引
        df.reset_index(drop=True, inplace=True)
    
    print(f'最终获取 {len(df)} 只股票实时行情 用时 {(time.time() - starttime_tick):>.2f} 秒')
    
    return df


def update_stockquote(code, df_history, df_today):
    """
    使用pytdx获取当前实时行情。合并历史DF和当前行情，返回合并后的DF
    :param code:  str类型。股票代码，'600030'
    :param df_history: DF类型。该股的历史DF数据
    :param df_today: DF类型。该股的当天盘中最新数据
    :return:合并后的DF数据
    """
    now_date = pd.to_datetime(time.strftime("%Y-%m-%d", time.localtime()))
    # now_time = time.strftime("%H:%M:%S", time.localtime())
    
    # df_history[date]最后一格的日期小于今天
    if pd.to_datetime(df_history.at[df_history.index[-1], 'date']) < now_date:
        # 检查df_today是否为空以及是否包含'code'列
        if not df_today.empty and 'code' in df_today.columns:
            df_today = df_today[(df_today['code'] == code)]
            if not df_today.empty:
                with pd.option_context('mode.chained_assignment', None):  # 临时屏蔽语句警告
                    df_today['date'] = now_date
                df_today.set_index('date', drop=False, inplace=True)
                df_today = df_today.rename(columns={'price': 'close'})
                # 确保所有需要的列都存在
                required_columns = ['code', 'date', 'open', 'high', 'low', 'close', 'vol', 'amount']
                available_columns = [col for col in required_columns if col in df_today.columns]
                df_today = df_today[available_columns]
                result = pd.concat([df_history, df_today], axis=0, ignore_index=False)
                result = result.ffill().infer_objects(copy=False)  # 向下填充无效值，使用直接方法调用替代fillna(method='ffill')
                if '流通市值' in result.columns and '换手率' in result.columns:
                    result['流通市值'] = result['流通股'] * result['close']
                    result = result.round({'流通市值': 2, })  # 指定列四舍五入
                if '换手率' in result.columns:
                    result['换手率'] = result['vol'] / result['流通股'] * 100
                    result = result.round({'换手率': 2, })  # 指定列四舍五入
                return result
    
    # 如果不满足条件或df_today为空，返回原始的df_history
    return df_history


if __name__ == '__main__':
    stock_code = '600036'
    day2csv(ucfg.tdx['tdx_path'] + '/vipdoc/sh/lday', 'sh' + stock_code + '.day', ucfg.tdx['csv_lday'])
    df_gbbq = pd.read_csv(ucfg.tdx['csv_gbbq'] + '/gbbq.csv', encoding='gbk', dtype={'code': str})
    df_bfq = pd.read_csv(ucfg.tdx['csv_lday'] + os.sep + stock_code + '.csv',
                         index_col=None, encoding='gbk', dtype={'code': str})
    df_qfq = make_fq(stock_code, df_bfq, df_gbbq)
    if len(df_qfq) > 0:
        df_qfq.to_csv(ucfg.tdx['csv_lday'] + os.sep + stock_code + '.csv', index=False, encoding='gbk')
