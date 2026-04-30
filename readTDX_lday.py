#!/usr/bin/python
# -*- coding: UTF-8 -*-
"""
在网络读取通达信代码上修改加工，增加、完善了一些功能
1、增加了读取深市股票功能。
2、增加了在已有数据的基础上追加最新数据，而非完全删除重灌。
3、增加了读取上证指数、沪深300指数功能。
4、没有使用pandas库，但输出的CSV格式是pandas库的dataFrame格式。
5、过滤了无关的债券指数、板块指数等，只读取沪市、深市A股股票。

数据单位：金额（元），成交量（股）

作者：wking [http://wkings.net]
"""
import os
import sys
import time
import pandas as pd
import argparse
from tqdm import tqdm
from multiprocessing import Pool, RLock, freeze_support
import func
import user_config as ucfg


def check_files_exist():
    # 判断目录和文件是否存在。不存在则创建，存在且运行带有del参数则删除。
    # 创建必要的目录结构
    os.makedirs(ucfg.tdx['csv_lday'], exist_ok=True)
    os.makedirs(ucfg.tdx['csv_index'], exist_ok=True)
    os.makedirs(ucfg.tdx['pickle'], exist_ok=True)
    
    if 'del' in str(sys.argv[1:]):
        # 删除现有文件并重新生成完整数据
        for root, dirs, files in os.walk(ucfg.tdx['csv_lday'], topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
        
        for root, dirs, files in os.walk(ucfg.tdx['csv_index'], topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
        
        for root, dirs, files in os.walk(ucfg.tdx['pickle'], topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))


def update_lday():
    # 使用akshare获取股票代码和名称的映射表
    stock_name_map = {}
    try:
        import akshare as ak
        print("正在获取股票代码和名称映射...")
        stock_code_name_df = ak.stock_info_a_code_name()
        if not stock_code_name_df.empty:
            # 创建股票代码到名称的映射字典
            stock_name_map = dict(zip(stock_code_name_df['code'], stock_code_name_df['name']))
            print(f"成功获取 {len(stock_name_map)} 只股票的名称映射")
        else:
            print("akshare未返回数据，将使用空的股票名称映射")
    except Exception as e:
        print(f"获取股票名称映射失败: {e}，将使用空的股票名称映射")
    
    # 读取通达信正常交易状态的股票列表。infoharbor_spec.cfg退市文件不齐全，放弃使用
    tdx_stocks_path = os.path.join(ucfg.tdx['tdx_path'], 'T0002', 'hq_cache', 'infoharbor_ex.code')
    tdx_stocks = pd.read_csv(tdx_stocks_path,
                             sep='|', header=None, index_col=None, encoding='gbk', dtype={0: str})
    file_listsh = tdx_stocks[0][tdx_stocks[0].apply(lambda x: x[0:1] == "6")]
    file_listsz = tdx_stocks[0][tdx_stocks[0].apply(lambda x: x[0:1] != "6")]

    print("从通达信深市股票导出数据")
    sz_lday_path = os.path.join(ucfg.tdx['tdx_path'], 'vipdoc', 'sz', 'lday')
    for f in tqdm(file_listsz):
        f = 'sz' + f + '.day'
        sz_file_path = os.path.join(sz_lday_path, f)
        if os.path.exists(sz_file_path):  # 处理深市sh00开头和创业板sh30文件，否则跳过此次循环
            func.day2csv(sz_lday_path, f, ucfg.tdx['csv_lday'], stock_name_map)

    print("从通达信导出沪市股票数据")
    sh_lday_path = os.path.join(ucfg.tdx['tdx_path'], 'vipdoc', 'sh', 'lday')
    for f in tqdm(file_listsh):
        # 处理沪市sh6开头文件，否则跳过此次循环
        f = 'sh' + f + '.day'
        sh_file_path = os.path.join(sh_lday_path, f)
        if os.path.exists(sh_file_path):
            func.day2csv(sh_lday_path, f, ucfg.tdx['csv_lday'], stock_name_map)

    print("从通达信导出指数数据")
    sz_lday_path = os.path.join(ucfg.tdx['tdx_path'], 'vipdoc', 'sz', 'lday')
    sh_lday_path = os.path.join(ucfg.tdx['tdx_path'], 'vipdoc', 'sh', 'lday')
    for i in tqdm(ucfg.index_list):
        if 'sh' in i:
            func.day2csv(sh_lday_path, i, ucfg.tdx['csv_index'], stock_name_map)
        elif 'sz' in i:
            func.day2csv(sz_lday_path, i, ucfg.tdx['csv_index'], stock_name_map)


def qfq(file_list, df_gbbq, cw_dict, tqdm_position=None):
    tq = tqdm(file_list, leave=False, position=tqdm_position)
    csv_lday_path = ucfg.tdx['csv_lday']
    pickle_path = ucfg.tdx['pickle']
    failed_stocks = []
    
    for filename in tq:
        try:
            # process_info = f'[{(file_list.index(filename) + 1):>4}/{str(len(file_list))}] {filename}'
            csv_file_path = os.path.join(csv_lday_path, filename)
            df_bfq = pd.read_csv(csv_file_path, index_col=None, encoding='gbk',
                                 dtype={'code': str})
            df_qfq = func.make_fq(filename[:-4], df_bfq, df_gbbq, cw_dict)
            # lefttime_tick = int((time.time() - starttime_tick) / (file_list.index(filename) + 1) * (len(file_list) - (file_list.index(filename) + 1)))
            if isinstance(df_qfq, pd.DataFrame) and len(df_qfq) > 0:  # 返回值是DataFrame且行数大于0
                # 写入csv和pkl文件，无论是否有更新
                df_qfq.to_csv(csv_file_path, index=False, encoding='gbk')
                pkl_file_path = os.path.join(pickle_path, filename[:-4] + '.pkl')
                df_qfq.to_pickle(pkl_file_path)
                tq.set_description(filename + "复权完成")
            else:
                tq.set_description(filename + "复权失败")
                failed_stocks.append(filename)
        except Exception as e:
            tq.set_description(filename + f"复权异常: {str(e)[:20]}")
            failed_stocks.append(filename)
    
    if failed_stocks:
        print(f"\n复权失败的股票数量: {len(failed_stocks)}")
        print("失败列表:")
        for stock in failed_stocks[:10]:
            print(f"  {stock}")
        if len(failed_stocks) > 10:
            print(f"  ... 还有 {len(failed_stocks) - 10} 只股票失败")
        #     print(f'{process_info} 无需更新 已用{(time.time() - starttime_tick):.2f}秒 剩余预计{lefttime_tick}秒')


if __name__ == '__main__':
    if 'del' in str(sys.argv[1:]):
        print('检测到参数del, 删除现有文件并重新生成完整数据')
    else:
        print('附带命令行参数 readTDX_lday.py del 删除现有文件并重新生成完整数据')
    if 'single' in sys.argv[1:]:
        print(f'检测到参数 single, 单进程执行')
    else:
        print(f'附带命令行参数 single 单进程执行(默认多进程)')
    # print('参数列表:', str(sys.argv[1:]))
    # print('脚本名:', str(sys.argv[0]))

    # 主程序开始
    check_files_exist()
    update_lday()
    # 通达信文件处理完成

    # 处理生成的通达信日线数据，复权加工代码
    file_list = os.listdir(ucfg.tdx['csv_lday'])
    starttime_tick = time.time()
    gbbq_path = os.path.join(ucfg.tdx['csv_gbbq'], 'gbbq.csv')
    df_gbbq = pd.read_csv(gbbq_path, encoding='gbk', dtype={'code': str})
    cw_dict = func.readall_local_cwfile()

    if 'single' in sys.argv[1:]:
        qfq(file_list, df_gbbq, cw_dict)
    else:
        # 多进程
        # print('Parent process %s' % os.getpid())
        # 进程数 读取CPU逻辑处理器个数
        if os.cpu_count() > 8:
            t_num = int(os.cpu_count() / 1.5)
        else:
            t_num = os.cpu_count() - 2

        div, mod = int(len(file_list) / t_num), len(file_list) % t_num
        freeze_support()  # for Windows support
        tqdm.set_lock(RLock())  # for managing output contention
        p = Pool(processes=t_num, initializer=tqdm.set_lock, initargs=(tqdm.get_lock(),))
        for i in range(0, t_num):
            if i + 1 != t_num:
                # print(i, i * div, (i + 1) * div)
                p.apply_async(qfq, args=(file_list[i * div:(i + 1) * div], df_gbbq, cw_dict, i))
            else:
                # print(i, i * div, (i + 1) * div + mod)
                p.apply_async(qfq, args=(file_list[i * div:(i + 1) * div + mod], df_gbbq, cw_dict, i))
        p.close()
        p.join()

    print('日线数据全部处理完成')
