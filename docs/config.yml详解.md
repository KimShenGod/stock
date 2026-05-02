# config.yml 配置详解

本文档详细说明 `config.yml` 各配置项的含义和用法。

## 文件结构

```yaml
version: 0.1.6
whitelist: [base, extra, validator, mod]

base:
  # RQAlpha基础配置
  
extra:
  # RQAlpha扩展配置
  
strategies:
  # 策略组合配置
  
backtest:
  # 向量化回测配置
```

## RQAlpha配置（base/extra）

这些配置用于 RQAlpha 回测框架。

### base 配置项

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| data_bundle_path | ~ | 数据包路径 |
| strategy_file | strategy.py | 策略文件路径 |
| start_date | 2015-06-01 | 回测起始日期 |
| end_date | 2050-01-01 | 回测结束日期 |
| run_type | b | 运行类型：b=回测，p=模拟，r=实盘 |
| frequency | 1d | 频率：1d=日线，1m=分钟线 |
| accounts | stock/future | 账户类型配置 |
| forced_liquidation | true | 是否强平 |

### extra 配置项

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| log_level | info | 日志级别 |
| enable_profiler | false | 性能分析开关 |
| log_file | ~ | 日志输出文件 |

## 策略组合配置（strategies）

定义策略组合，组合内策略为AND关系。

### 可用策略列表

- 高开涨停
- 前日涨停
- 今日涨停
- 小市值
- 换手率
- MACD周线金叉
- MACD日线金叉
- 连续上涨
- MACD周线区间（别名：周线MACD区间）

### 配置示例

```yaml
strategies:
  # 默认组合
  default: ["周线MACD区间", "小市值"]
  
  # 自定义组合1
  combo_1: ["前日涨停", "小市值"]
  
  # 自定义组合2
  combo_2: ["高开涨停", "换手率"]
  
  # 自定义组合3
  combo_3: ["今日涨停", "小市值"]
```

### 使用组合

命令行指定：

```bash
# 使用默认组合
python run_vectorized_backtest.py --strategy-combo default

# 使用自定义组合
python run_vectorized_backtest.py --strategy-combo combo_1
```

## 向量化回测配置（backtest）

### 资金配置

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| initial_capital | 400000 | 初始资金（元）|
| max_positions | 3 | 最大持仓数量 |

### 风控配置

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| stop_loss | 0.08 | 止损比例（8%亏损）|
| take_profit | 0.30 | 止盈比例（30%盈利）|
| max_hold_days | 30 | 最大持仓天数 |

### 成本配置

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| commission_rate | 0.0003 | 佣金率（0.03%）|
| stamp_tax_rate | 0.001 | 印花税率（0.1%，仅卖出）|

### 路径配置

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| signal_dir | ./backtest_results/signals | 信号文件目录 |
| output_dir | ./backtest_results | 输出结果目录 |

### 批处理配置

| 配置项 | 默认值 | 说明 |
|-------|--------|-----|
| batch_days | 30 | 批处理天数 |

## 完整配置示例

```yaml
version: 0.1.6
whitelist: [base, extra, validator, mod]

base:
  data_bundle_path: ~
  strategy_file: strategy.py
  start_date: 2015-06-01
  end_date: 2050-01-01
  run_type: b
  frequency: 1d
  accounts:
    stock: ~
    future: ~
  forced_liquidation: true

extra:
  log_level: info
  enable_profiler: false

strategies:
  default: ["周线MACD区间", "小市值"]
  combo_1: ["前日涨停", "小市值"]
  combo_2: ["高开涨停", "换手率"]

backtest:
  initial_capital: 400000
  max_positions: 3
  stop_loss: 0.08
  take_profit: 0.30
  max_hold_days: 30
  commission_rate: 0.0003
  stamp_tax_rate: 0.001
  signal_dir: "./backtest_results/signals"
  output_dir: "./backtest_results"
  batch_days: 30
```

## 配置优先级

运行时配置优先级：

1. 命令行参数（最高）
2. config.yml文件
3. 代码默认值（最低）

示例：

```bash
# 命令行可覆盖配置文件
python run_vectorized_backtest.py --start 20240101 --end 20241231 --strategy-combo combo_1
```

## 添加新策略组合

1. 在 `strategy_registry.py` 中注册新策略
2. 在 `config.yml` 中添加组合配置

```yaml
strategies:
  # 新组合
  my_combo: ["我的策略1", "小市值"]
```

## 配置验证

运行时自动验证配置：

- 资金必须为正数
- 持仓数量必须为正整数
- 止损/止盈比例必须在合理范围
- 策略名称必须存在于注册表

无效配置将抛出异常并提示修正。
