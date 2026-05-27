# zhihu_quant_screener.py 使用教程与功能说明

## 1. 脚本定位

`zhihu_quant_screener.py` 是一个 A 股量化筛选脚本，核心思路是：

- 基础面硬过滤（市值、流动性、估值区间）
- 技术面形态识别（趋势突破、缩量回踩低吸）
- 可选财务面加严（Tushare：ROE、营收同比、净利同比、资产负债率）
- 多维打分后输出候选股票列表

适合做每日盘后/盘中的第一轮股票池筛选。

## 2. 主要功能

- 仅主板筛选（默认，可切换包含创业板/科创板/北交所）
- 自动过滤 ST / 退市风险名称
- 趋势判断：`MA20/MA60/MA120`、`MACD`
- 形态识别：
  - 趋势突破（接近/突破 60 日高点并放量）
  - 缩量回踩低吸（回踩均线 + 缩量特征）
- 风险惩罚（涨幅过大、离高点过远、异常放量等）
- 输出 `CSV + XLSX`

## 3. 数据源说明（已做兜底）

### 3.1 实时行情（股票池）

- 首选：AkShare `stock_zh_a_spot_em`
- 回退：AkShare 分市场接口
- 再回退：AkShare `stock_zh_a_spot`
- 再回退：腾讯行情 direct 接口
- 再回退：东方财富 direct 接口
- 最后兜底：仅代码+名称列表模式（无实时估值/成交额字段）

### 3.2 历史日线（K线）

- 首选：东方财富日线 direct 接口
- 回退：AkShare `stock_zh_a_hist`
- 再回退：新浪日线接口
- 本地缓存：默认保存到 `data/kline/*.csv`
- 更新方式：默认增量更新，只补拉最近缺失区间，不会每次全量重拉

## 4. 环境准备

建议 Python 3.10+。

安装依赖：

```bash
pip install akshare pandas numpy tqdm openpyxl tushare requests
```

说明：

- `tushare` 仅在你使用 `--tushare-token` 时需要。
- `openpyxl` 用于写出 `.xlsx`，缺失时不影响 `.csv` 输出。

## 5. 快速开始

在脚本所在目录执行：

```bash
python zhihu_quant_screener.py
```

默认会：

- 拉取 A 股实时行情
- 根据默认阈值过滤
- 增量更新最近约 `430` 天历史数据到本地缓存后再打分
- 输出最新结果 `screen_result.csv`（并尽量输出 `screen_result.xlsx`）
- 同时归档一份到 `data/result_archive/`（文件名带日期时间戳）

## 5.1 缓存目录

默认缓存目录：

```text
data/
  kline/
    000001.csv
    000002.csv
    600000.csv
```

可通过参数修改：

```bash
python zhihu_quant_screener.py --data-dir my_data
```

## 6. 常用命令模板

### 6.1 平衡版（推荐起步）

```bash
python zhihu_quant_screener.py --min-market-cap-yi 100 --min-score 70 --workers 6
```

### 6.2 严格版（更少但更强）

```bash
python zhihu_quant_screener.py ^
  --min-market-cap-yi 300 ^
  --min-avg-amount-yi 2 ^
  --min-score 78 ^
  --require-bull-ma ^
  --require-macd-positive ^
  --require-breakout-or-low-absorb ^
  --workers 6
```

### 6.3 启用 Tushare 基本面硬过滤

```bash
python zhihu_quant_screener.py ^
  --tushare-token YOUR_TOKEN ^
  --min-roe 12 ^
  --min-netprofit-yoy 15 ^
  --min-revenue-yoy 10
```

### 6.4 小样本调试（只跑前 N 只）

```bash
python zhihu_quant_screener.py --limit 100 --workers 1 --screen-workers 8 --min-score 60
```

### 6.5 只用本地缓存，不联网更新

```bash
python zhihu_quant_screener.py --skip-kline-update
```

### 6.6 强制重新拉取历史日线

```bash
python zhihu_quant_screener.py --force-kline-update
```

## 7. 关键参数说明

### 7.1 股票池与硬过滤

- `--main-board-only`：仅主板（默认开启）
- `--include-non-main-board`：包含非主板
- `--min-market-cap-yi`：最小总市值（亿元）
- `--min-avg-amount-yi`：20 日最低平均成交额（亿元）
- `--min-price` / `--max-price`：价格区间
- `--max-pe` / `--max-pb`：估值上限

### 7.2 技术面阈值

- `--ma120-tolerance-pct`：MA60 相对 MA120 的容忍下穿比例
- `--breakout-near-pct`：距 60 日高点多近算“近突破”
- `--breakout-min-volume-ratio`：突破时最低量比
- `--breakout-min-pct-chg`：突破时最低涨幅

### 7.3 缩量低吸参数

- `--low-absorb-lookback-days`：低吸量能比较窗口
- `--low-absorb-recent-days`：最近观察区间
- `--low-volume-percentile`：低量分位阈值
- `--low-absorb-min-hit-days`：命中最少天数
- `--pullback-near-ma-pct`：回踩均线的偏离容忍

### 7.4 严格开关

- `--require-bull-ma`：必须均线多头趋势达标
- `--require-macd-positive`：必须 MACD 零轴上方
- `--require-breakout-or-low-absorb`：必须是突破或低吸形态

### 7.5 运行控制

- `--min-score`：最低分数线
- `--workers`：日线缓存更新并发线程数
- `--screen-workers`：筛选计算并发线程数（默认 `8`）
- `--limit`：仅处理前 N 只（调试）
- `--history-days`：拉取历史天数
- `--adjust`：复权方式（`qfq`/`hfq`/空）
- `--data-dir`：本地缓存目录
- `--candidate-output`：候选池缓存路径
- `--refresh-candidates`：强制刷新候选池缓存
- `--skip-kline-update`：跳过联网更新，仅使用已有缓存
- `--force-kline-update`：忽略缓存，强制重新拉取
- `--end-date`：指定统计截止日期
- `--include-today` / `--exclude-today`：控制默认是否包含当日
- `--spot-retry` / `--spot-retry-sleep`：实时行情重试配置
- `--output`：输出文件名

## 8. 输出结果说明

脚本会输出：

- 终端打印 Top 50
- `screen_result.csv`
- `screen_result.xlsx`（若环境支持）

常见字段：

- `score`：综合分（0-100）
- `grade`：等级（A/B/C/D）
- `pattern`：形态分类（如趋势突破、缩量回踩低吸）
- `reasons`：打分与扣分原因摘要
- `ma*`、`dif/dea/macd_hist`：技术指标
- `volume_ratio_5`、`drawdown_120d_pct`：量价与风险位置信息

## 9. 常见问题

### 9.1 没有结果怎么办？

可尝试：

- 降低 `--min-score`
- 放宽 `--min-market-cap-yi`、`--min-avg-amount-yi`
- 关闭严格开关（`--require-*`）

补充：在“本地缓存评分 + `20250322 ~ 20260526`”这一区间的实测中（`--skip-kline-update`）：

- `--min-score 65`：结果为 `0` 只
- `--min-score 60`：结果约 `21` 只
- `--min-score 55`：结果约 `30` 只

可直接使用：

```bash
python zhihu_quant_screener.py --skip-kline-update --end-date 20260526 --min-score 60 --workers 1 --screen-workers 8
```

若仍无结果，再降到 `--min-score 55`。

另外，如果运行时出现以下警告：

- `当前行情源不含 total_mv`
- `当前行情源不含 PE`
- `当前行情源不含 PB`

说明部分估值/市值加分项会缺失，整体分数会偏低，`--min-score` 需要相应下调。

### 9.2 数据拉取报错怎么办？

- 增加重试：`--spot-retry 8 --spot-retry-sleep 2`
- 降低并发：`--workers 1`
- 更换网络环境后重试

### 9.3 Tushare 相关字段为空？

若未配置 `--tushare-token`，脚本会仅按行情+技术面运行，这是正常行为。

## 10. 免责声明

本脚本仅用于量化研究与策略辅助，不构成任何投资建议。请结合独立研究与风险控制决策。
