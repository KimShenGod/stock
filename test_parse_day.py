import struct

"""
测试文件：解析32字节的day文件记录

功能：
1. 接受用户输入的32字节十六进制数据
2. 解析数据并输出各个字段
3. 提供解析说明
"""

def parse_32byte_record(hex_data):
    """
    解析32字节的day文件记录
    
    参数:
    hex_data: str - 32字节的十六进制数据
    
    返回:
    dict - 解析后的记录数据
    """
    try:
        # 移除空格和换行符并转换为字节串
        hex_data = hex_data.replace(' ', '').replace('\n', '').replace('\r', '')
        if len(hex_data) != 64:  # 32字节 = 64个十六进制字符
            print(f"错误：输入数据长度不正确，需要32字节（64个十六进制字符），实际长度：{len(hex_data)}字符")
            return None
        
        # 转换为字节串
        record = bytes.fromhex(hex_data)
        
        # 固定使用32字节格式读取
        # 格式：<IIIIIfII
        # 字段：日期, 开盘价, 最高价, 最低价, 收盘价, 成交额, 成交量, 预留
        date, open_price, high_price, low_price, close_price, amount, volume, reserve = struct.unpack('<IIIIIfII', record)
        
        # 价格转换（实际价格 = 存储值 / 100）
        open_price = open_price / 100
        high_price = high_price / 100
        low_price = low_price / 100
        close_price = close_price / 100
        
        # 构建返回数据
        result = {
            'date': date,
            'open_price': open_price,
            'high_price': high_price,
            'low_price': low_price,
            'close_price': close_price,
            'amount': amount,
            'volume': volume,
            'reserve': reserve
        }
        
        return result
    except Exception as e:
        print(f"解析记录失败: {e}")
        return None

def print_record_details(record):
    """
    打印记录的详细信息
    
    参数:
    record: dict - 解析后的记录数据
    """
    if record:
        print("=== 32字节day文件记录解析结果 ===")
        print(f"日期: {record['date']}")
        print(f"开盘价: {record['open_price']:.2f}")
        print(f"最高价: {record['high_price']:.2f}")
        print(f"最低价: {record['low_price']:.2f}")
        print(f"收盘价: {record['close_price']:.2f}")
        print(f"成交额: {record['amount']:.2f}")
        print(f"成交量: {record['volume']}")
        print(f"预留字段: {record['reserve']}")
        print()
        print("=== 解析说明 ===")
        print("1. day文件格式: 每条记录固定32字节")
        print("2. 数据顺序: 最新数据在文件开头，最旧数据在文件末尾")
        print("3. 字段解析:")
        print("   - 日期: YYYYMMDD格式的整数")
        print("   - 价格: 存储值 = 实际价格 × 100")
        print("   - 成交额: 浮点型，单位元")
        print("   - 成交量: 整型，单位手")
        print("   - 预留字段: 固定为0")
        print("4. 解析方法: 使用struct.unpack('<IIIIIfII')进行解析")
        print("   - <: 小端序")
        print("   - I: 无符号整型（4字节）")
        print("   - f: 浮点型（4字节）")
        print("   - II: 两个无符号整型（8字节）")
    else:
        print("无法解析记录")

if __name__ == "__main__":
    # 示例数据
    example_data = '''3326 3501 1b03 0000 2003 0000 0103 0000
0203 0000 6294 2e4c 55e6 0000 0000 0000'''    
    
    print("=== 32字节day文件记录解析工具 ===")
    print(f"示例输入: {example_data}")
    print("请输入32字节的十六进制数据（可以包含空格和换行，输入空行结束）:")
    
    # 获取多行用户输入
    user_input_lines = []
    while True:
        line = input().strip()
        if not line:
            break
        user_input_lines.append(line)
    
    # 合并所有行
    user_input = ' '.join(user_input_lines)
    
    # 如果用户没有输入，使用示例数据
    if not user_input:
        user_input = example_data
    
    # 解析数据
    record = parse_32byte_record(user_input)
    
    # 打印记录详情
    print_record_details(record)