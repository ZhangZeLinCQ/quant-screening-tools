#!/usr/bin/env python3
"""Low-volume pullback screener for Shanghai/Shenzhen main-board stocks.

The screener keeps its own stock universe, daily K-line cache, and result
snapshots under this directory. It prefers AkShare for the candidate universe
and uses Eastmoney for daily K-line history.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

try:
    import akshare as ak
except Exception:  # pragma: no cover - optional dependency
    ak = None


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
KLINE_DIR = DATA_DIR / "kline"
OUTPUT_DIR = PROJECT_DIR / "output"
CANDIDATE_TABLE_PATH = DATA_DIR / "candidate_table.csv"
RUN_STATE_PATH = DATA_DIR / "run_state.json"
LATEST_RESULT_PATH = OUTPUT_DIR / "latest_low_absorb.csv"
LATEST_RESULT_MD_PATH = OUTPUT_DIR / "latest_low_absorb.md"

# 已退市/合并股票，数据源无法获取日线，统一跳过
EXCLUDED_STOCKS: set[str] = {
    "601299",  # 中国北车（已合并为中国中车）
    "000562",  # 宏源证券（已合并为申万宏源）
    "000024",  # 招商地产（已退市）
}

EASTMONEY_LIST_URLS = (
    "https://push2his.eastmoney.com/api/qt/clist/get",
    "https://push2.eastmoney.com/api/qt/clist/get",
    "https://82.push2.eastmoney.com/api/qt/clist/get",
)
EASTMONEY_KLINE_URL = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
TENCENT_QUOTE_URL = "https://qt.gtimg.cn/q="
SINA_KLINE_URL = "https://quotes.sina.cn/cn/api/jsonp_v2.php/var%20_kline_/CN_MarketDataService.getKLineData"
EASTMONEY_QUOTE_UT = "fa5fd1943c7b386f172d6893dbfba10b"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

MAIN_BOARD_PREFIXES = ("600", "601", "603", "605", "000", "001", "002", "003")
DEFAULT_HISTORY_DAYS = 730
DEFAULT_MIN_MARKET_CAP_YI = 500.0
DEFAULT_LOOKBACK_DAYS = 240
DEFAULT_LOWEST_GREEN_PERCENT = 10.0
DEFAULT_RECENT_DAYS = 10
DEFAULT_MIN_HIT_DAYS = 3
DEFAULT_LIMIT_PCT_THRESHOLD = 9.8
# 下面这些增强过滤默认关闭；传入大于 0 的参数才会生效。
DEFAULT_MIN_AVG_AMOUNT_YI = 0.0
DEFAULT_RECENT_VOLUME_SHRINK_RATIO = 0.0
DEFAULT_MAX_20D_DROP_PCT = 0.0
DEFAULT_MIN_DISTANCE_FROM_60D_LOW_PCT = 0.0
DEFAULT_MAX_LATEST_VOLUME_PERCENTILE = 0.0
DEFAULT_MIN_LOW_ABSORB_SCORE = 0.0
DEFAULT_MIN_FINAL_SCORE = 0.0
DEFAULT_MIN_REWARD_RISK_RATIO = 0.0
DEFAULT_DOWN_DAY_MODE = "pct"
# 研究报告增强：主题景气度与大盘环境过滤默认关闭，可通过参数打开。
DEFAULT_THEME_CSV = "config/hot_theme_codes.csv"
DEFAULT_THEME_SCORE_WEIGHT = 10.0
DEFAULT_MIN_THEME_SCORE = 0.0
DEFAULT_MARKET_FILTER = "off"
DEFAULT_MARKET_INDEX_CODE = "000001"
DEFAULT_WORKERS = 6
DEFAULT_COMPUTE_WORKERS = 1
DEFAULT_AFTER_HOUR = 16
DEFAULT_MAX_FULL_HISTORY_BARS = 900
TENCENT_BATCH_SIZE = 80


@dataclass(frozen=True)
class StockCandidate:
    code: str
    name: str
    market: str
    secid: str
    total_market_cap_yi: float
    latest_price: float | None
    latest_trade_date: str
    industry: str = ""
    concepts: str = ""


@dataclass(frozen=True)
class KlineRow:
    trade_date: dt.date
    open_price: float
    close_price: float
    high_price: float
    low_price: float
    volume: int
    amount: float
    amplitude_pct: float | None
    pct_change: float | None
    change_amount: float | None
    turnover_rate_pct: float | None

    @property
    def volume_color(self) -> str:
        return "红" if self.close_price >= self.open_price else "绿"


@dataclass(frozen=True)
class ScreenResult:
    code: str
    name: str
    market: str
    industry: str
    concepts: str
    theme_tags: list[str]
    theme_score: float
    total_market_cap_yi: float
    recent_window_days: int
    hit_days: int
    hit_dates: list[str]
    hit_green_volumes: list[int]
    hit_green_ranks: list[int]
    hit_green_percentiles: list[float]
    latest_volume: int | None
    latest_volume_rank: int | None
    latest_volume_percentile: float | None
    latest_is_down_day: bool
    latest_down_volume_rank: int | None
    latest_down_volume_percentile: float | None
    latest_volume_to_cutoff_ratio: float | None
    green_cutoff_volume: int
    green_sample_count: int
    excluded_limit_days: int
    avg_amount_20d_yi: float | None
    recent_volume_ratio: float | None
    drop_20d_pct: float | None
    distance_from_60d_low_pct: float | None
    confirm_signal: str
    latest_hit_low_price: float | None
    not_break_low_volume_low: bool
    ma5: float | None
    ma10: float | None
    ma20: float | None
    ma60: float | None
    price_above_ma5: bool
    price_above_ma10: bool
    price_above_ma20: bool
    stop_price: float | None
    target_price: float | None
    reward_risk_ratio: float | None
    low_absorb_score: float
    final_score: float
    exhaustion_score: float
    shrink_score: float
    position_score: float
    trend_score: float
    confirm_score: float
    reward_risk_score: float
    suggested_status: str
    lookback_start: str
    lookback_end: str
    latest_close: float | None
    latest_pct_change: float | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "筛选沪深主板 10cm 当前低吸票：市值大于阈值、非 ST，"
            "最近观察窗口内至少 K 个交易日落在过去 X 天的下跌日低量分位，"
            "并可选成交额、破位、缩量、确认信号过滤。"
        )
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=DEFAULT_LOOKBACK_DAYS,
        help=f"成交量排序回看交易日数，必须大于等于 30，默认 {DEFAULT_LOOKBACK_DAYS}",
    )
    parser.add_argument(
        "--lowest-down-percent",
        "--lowest-green-percent",
        dest="lowest_green_percent",
        type=float,
        default=DEFAULT_LOWEST_GREEN_PERCENT,
        help=(
            f"下跌日成交量最低百分比，常用 5-10，默认 {DEFAULT_LOWEST_GREEN_PERCENT}。"
            "--lowest-green-percent 为兼容旧参数名。"
        ),
    )
    parser.add_argument(
        "--down-day-mode",
        choices=("pct", "green"),
        default=DEFAULT_DOWN_DAY_MODE,
        help=(
            "下跌日口径：pct=涨跌幅<0，green=收盘价<开盘价。"
            f"默认 {DEFAULT_DOWN_DAY_MODE}，更适合卖盘枯竭筛选。"
        ),
    )
    parser.add_argument(
        "--recent-days",
        type=int,
        default=DEFAULT_RECENT_DAYS,
        help=f"最近观察窗口交易日数，默认 {DEFAULT_RECENT_DAYS}",
    )
    parser.add_argument(
        "--min-hit-days",
        type=int,
        default=DEFAULT_MIN_HIT_DAYS,
        help=f"最近观察窗口内至少命中的下跌低量交易日数，默认 {DEFAULT_MIN_HIT_DAYS}",
    )
    parser.add_argument(
        "--limit-pct-threshold",
        type=float,
        default=DEFAULT_LIMIT_PCT_THRESHOLD,
        help=f"识别涨停/跌停的涨跌幅阈值，默认 {DEFAULT_LIMIT_PCT_THRESHOLD}，即绝对涨跌幅 >= 9.8%% 会被排除",
    )
    parser.add_argument(
        "--keep-limit-days",
        action="store_true",
        help="默认会从历史排序样本和最近命中样本中排除涨跌停日；加此参数则保留涨跌停日",
    )
    parser.add_argument(
        "--min-avg-amount-yi",
        type=float,
        default=DEFAULT_MIN_AVG_AMOUNT_YI,
        help=(
            "近 20 日平均成交额下限，单位亿元；0 表示不启用。"
            f"默认 {DEFAULT_MIN_AVG_AMOUNT_YI}"
        ),
    )
    parser.add_argument(
        "--recent-volume-shrink-ratio",
        type=float,
        default=DEFAULT_RECENT_VOLUME_SHRINK_RATIO,
        help=(
            "近 5 日均量 / 近 60 日均量上限，用于确认整体缩量；0 表示不启用。"
            f"例如 0.7，默认 {DEFAULT_RECENT_VOLUME_SHRINK_RATIO}"
        ),
    )
    parser.add_argument(
        "--max-20d-drop-pct",
        type=float,
        default=DEFAULT_MAX_20D_DROP_PCT,
        help=(
            "最近收盘价相对近 20 日最高价的最大允许回撤百分比；0 表示不启用。"
            "例如 20 表示超过 -20%% 则剔除。"
        ),
    )
    parser.add_argument(
        "--min-distance-from-60d-low-pct",
        type=float,
        default=DEFAULT_MIN_DISTANCE_FROM_60D_LOW_PCT,
        help=(
            "最近收盘价距离近 60 日最低价的最小百分比；0 表示不启用。"
            "例如 3 表示贴近 60 日新低 3%% 以内则剔除。"
        ),
    )
    parser.add_argument(
        "--max-latest-volume-percentile",
        type=float,
        default=DEFAULT_MAX_LATEST_VOLUME_PERCENTILE,
        help=(
            "最新一个交易日成交量在回看样本中的最高允许分位；0 表示不启用。"
            "例如 20 表示最新成交量必须处于过去回看周期最低 20%% 以内。"
        ),
    )
    parser.add_argument(
        "--require-confirm-signal",
        action="store_true",
        help=(
            "只保留已经出现确认信号的股票。确认信号定义为低量日后 1-3 个交易日内"
            "出现放量阳线并收盘突破低量日最高价。默认不强制，只在结果中标记。"
        ),
    )
    parser.add_argument(
        "--require-not-break-low-volume-low",
        action="store_true",
        help="只保留最新收盘价没有跌破最近一次命中低量日最低价的股票。",
    )
    parser.add_argument(
        "--require-price-above-ma5",
        action="store_true",
        help="只保留最新收盘价站上 5 日均线的股票，用于避免连续阴跌中过早低吸。",
    )
    parser.add_argument(
        "--require-price-above-ma10",
        action="store_true",
        help="只保留最新收盘价站上 10 日均线的股票，比 MA5 更稳但更容易漏掉早期信号。",
    )
    parser.add_argument(
        "--require-price-above-ma20",
        action="store_true",
        help="只保留最新收盘价站上 20 日均线的股票，适合更保守的确认型筛选。",
    )
    parser.add_argument(
        "--min-reward-risk-ratio",
        type=float,
        default=DEFAULT_MIN_REWARD_RISK_RATIO,
        help=(
            "最低盈亏比；0 表示不启用。默认止损价=最近命中低量日低点*0.98，"
            "目标价=近 60 日最高价。例如 2 表示潜在上涨空间至少为风险的 2 倍。"
        ),
    )
    parser.add_argument(
        "--min-low-absorb-score",
        type=float,
        default=DEFAULT_MIN_LOW_ABSORB_SCORE,
        help=(
            "最低低吸综合评分；0 表示不启用。评分范围 0-100，"
            "综合卖盘枯竭、缩量、位置、趋势、确认信号和盈亏比。"
        ),
    )
    parser.add_argument(
        "--theme-csv",
        default=DEFAULT_THEME_CSV,
        help=(
            "主题股票池 CSV 路径，默认 config/hot_theme_codes.csv。"
            "可包含列：股票代码、股票名称、主题标签、主题评分、备注。"
        ),
    )
    parser.add_argument(
        "--theme-score-weight",
        type=float,
        default=DEFAULT_THEME_SCORE_WEIGHT,
        help=(
            "主题评分纳入最终排序的权重，0-50。默认 10，表示最终评分=低吸评分90%%+主题评分10%%。"
        ),
    )
    parser.add_argument(
        "--min-theme-score",
        type=float,
        default=DEFAULT_MIN_THEME_SCORE,
        help="最低主题景气度评分；0 表示不启用。适合只筛 AI算力/机器人/商业航天/工业母机等主线。",
    )
    parser.add_argument(
        "--require-theme",
        action="store_true",
        help="只保留命中主题股票池或主题关键词的股票。",
    )
    parser.add_argument(
        "--min-final-score",
        type=float,
        default=DEFAULT_MIN_FINAL_SCORE,
        help="最低最终评分；0 表示不启用。最终评分会叠加主题景气度权重。",
    )
    parser.add_argument(
        "--market-filter",
        choices=("off", "ma20", "ma60", "ma20-and-ma60", "ma20-or-ma60"),
        default=DEFAULT_MARKET_FILTER,
        help=(
            "大盘环境过滤。off=关闭；ma20=指数收盘站上20日线；ma60=站上60日线；"
            "ma20-and-ma60=同时站上；ma20-or-ma60=至少站上一条。默认 off。"
        ),
    )
    parser.add_argument(
        "--market-index-code",
        default=DEFAULT_MARKET_INDEX_CODE,
        help="大盘过滤使用的指数代码，默认 000001 上证指数；也可用 399001 深证成指或 000300 沪深300。",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=DEFAULT_HISTORY_DAYS,
        help=f"本地保存的历史自然日跨度，默认 {DEFAULT_HISTORY_DAYS}",
    )
    parser.add_argument(
        "--min-market-cap-yi",
        type=float,
        default=DEFAULT_MIN_MARKET_CAP_YI,
        help=f"总市值下限，单位亿元，默认 {DEFAULT_MIN_MARKET_CAP_YI}",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="统计截止日期 YYYYMMDD；默认按当前时间自动决定是否包含今日",
    )
    parser.add_argument(
        "--include-today",
        action="store_true",
        help="未指定 --end-date 时强制允许包含今日数据",
    )
    parser.add_argument(
        "--exclude-today",
        action="store_true",
        help="未指定 --end-date 时强制不包含今日数据",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_WORKERS,
        help=f"日线数据获取的并发线程数，默认 {DEFAULT_WORKERS}",
    )
    parser.add_argument(
        "--compute-workers",
        type=int,
        default=DEFAULT_COMPUTE_WORKERS,
        help=f"筛选计算的并发线程数，默认 {DEFAULT_COMPUTE_WORKERS}",
    )
    parser.add_argument(
        "--refresh-candidates",
        action="store_true",
        help="强制刷新候选表",
    )
    parser.add_argument(
        "--skip-kline-update",
        action="store_true",
        help="只使用本地已有 K 线，不联网更新日线",
    )
    parser.add_argument(
        "--force-kline-update",
        action="store_true",
        help="忽略本地日线缓存，重新拉取候选股最近两年日线",
    )
    parser.add_argument(
        "--force-renormalize",
        action="store_true",
        help="对已有K线缓存强制重新做成交量单位归一化（不重新拉取数据）",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="结果 CSV 路径，默认 low_absorb_screener/output/low_absorb_日期_参数.csv",
    )
    parser.add_argument(
        "--candidate-output",
        default=str(CANDIDATE_TABLE_PATH),
        help="候选表保存路径",
    )
    parser.add_argument(
        "--data-dir",
        default=str(DATA_DIR),
        help="数据保存目录",
    )
    args = parser.parse_args()
    validate_args(parser, args)
    resolve_paths_and_dates(args)
    return args


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.lookback_days < 30:
        parser.error("--lookback-days 必须大于等于 30")
    if args.lowest_green_percent <= 0 or args.lowest_green_percent > 100:
        parser.error("--lowest-green-percent 必须在 0 到 100 之间")
    if args.recent_days <= 0:
        parser.error("--recent-days 必须大于 0")
    if args.min_hit_days <= 0:
        parser.error("--min-hit-days 必须大于 0")
    if args.min_hit_days > args.recent_days:
        parser.error("--min-hit-days 不能大于 --recent-days")
    if args.limit_pct_threshold <= 0 or args.limit_pct_threshold > 20:
        parser.error("--limit-pct-threshold 必须在 0 到 20 之间")
    if args.min_avg_amount_yi < 0:
        parser.error("--min-avg-amount-yi 不能小于 0")
    if args.recent_volume_shrink_ratio < 0:
        parser.error("--recent-volume-shrink-ratio 不能小于 0")
    if args.max_20d_drop_pct < 0:
        parser.error("--max-20d-drop-pct 不能小于 0")
    if args.min_distance_from_60d_low_pct < 0:
        parser.error("--min-distance-from-60d-low-pct 不能小于 0")
    if args.max_latest_volume_percentile < 0 or args.max_latest_volume_percentile > 100:
        parser.error("--max-latest-volume-percentile 必须在 0 到 100 之间")
    if args.min_reward_risk_ratio < 0:
        parser.error("--min-reward-risk-ratio 不能小于 0")
    if args.min_low_absorb_score < 0 or args.min_low_absorb_score > 100:
        parser.error("--min-low-absorb-score 必须在 0 到 100 之间")
    if args.min_final_score < 0 or args.min_final_score > 100:
        parser.error("--min-final-score 必须在 0 到 100 之间")
    if args.theme_score_weight < 0 or args.theme_score_weight > 50:
        parser.error("--theme-score-weight 必须在 0 到 50 之间")
    if args.min_theme_score < 0 or args.min_theme_score > 100:
        parser.error("--min-theme-score 必须在 0 到 100 之间")
    if args.history_days < args.lookback_days:
        parser.error("--history-days 不能小于 --lookback-days")
    if args.min_market_cap_yi <= 0:
        parser.error("--min-market-cap-yi 必须大于 0")
    if args.workers <= 0:
        parser.error("--workers 必须大于 0")
    if args.include_today and args.exclude_today:
        parser.error("--include-today 和 --exclude-today 不能同时使用")
    if args.end_date:
        parse_yyyymmdd(args.end_date)


def resolve_paths_and_dates(args: argparse.Namespace) -> None:
    data_dir = Path(args.data_dir).resolve()
    args.data_dir_path = data_dir
    args.kline_dir_path = data_dir / "kline"
    args.candidate_output_path = Path(args.candidate_output)
    if not args.candidate_output_path.is_absolute():
        args.candidate_output_path = (PROJECT_DIR / args.candidate_output_path).resolve()
    args.theme_csv_path = Path(args.theme_csv)
    if not args.theme_csv_path.is_absolute():
        args.theme_csv_path = (PROJECT_DIR / args.theme_csv_path).resolve()

    args.effective_end_date = resolve_effective_end_date(args)
    if args.output is None:
        args.output_path = OUTPUT_DIR / (
            f"low_absorb_{args.effective_end_date:%Y%m%d}"
            f"_d{args.lookback_days}"
            f"_g{format_percent_for_filename(args.lowest_green_percent)}"
            f"_r{args.recent_days}"
            f"_h{args.min_hit_days}.csv"
        )
    else:
        args.output_path = Path(args.output)
    if not args.output_path.is_absolute():
        args.output_path = (PROJECT_DIR / args.output_path).resolve()


def resolve_effective_end_date(args: argparse.Namespace) -> dt.date:
    now = dt.datetime.now()
    if args.end_date:
        return parse_yyyymmdd(args.end_date)
    if args.include_today:
        return now.date()
    if args.exclude_today:
        return now.date() - dt.timedelta(days=1)
    if now.hour >= DEFAULT_AFTER_HOUR:
        return now.date()
    return now.date() - dt.timedelta(days=1)


def parse_yyyymmdd(value: str) -> dt.date:
    return dt.datetime.strptime(value, "%Y%m%d").date()


def format_percent_for_filename(value: float) -> str:
    text = f"{value:g}".replace(".", "p")
    return text


def create_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            # 东方财富部分节点对复用连接比较敏感，直接关闭 keep-alive 可减少
            # RemoteDisconnected('Remote end closed connection without response')。
            "Connection": "close",
            "Referer": "https://quote.eastmoney.com/",
            "Origin": "https://quote.eastmoney.com",
        }
    )
    return session


def request_json(
    session: requests.Session,
    url: str,
    params: dict[str, Any],
    retries: int = 3,
    timeout: int = 20,
) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(retries):
        response = None
        try:
            response = session.get(url, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
        finally:
            if response is not None:
                response.close()
    raise RuntimeError(f"请求失败: {url}; {last_error}") from last_error


def is_main_board_code(code: str) -> bool:
    return code.startswith(MAIN_BOARD_PREFIXES)


def is_non_st_name(name: str) -> bool:
    upper_name = name.upper()
    return "ST" not in upper_name


def secid_for_code(code: str) -> str:
    market_id = "1" if code.startswith("6") else "0"
    return f"{market_id}.{code}"


def market_name(code: str) -> str:
    return "上海主板" if code.startswith("6") else "深圳主板"


def fetch_candidate_table(min_market_cap_yi: float) -> list[StockCandidate]:
    errors: list[str] = []

    if ak is not None:
        try:
            rows = fetch_candidate_table_via_akshare(min_market_cap_yi)
            if rows:
                return rows
            errors.append("AkShare 返回候选表为空")
        except Exception as exc:
            errors.append(f"AkShare 候选表获取失败: {exc}")
    else:
        errors.append("当前 Python 环境未安装 AkShare")

    # AkShare 的 A 股实时列表底层通常仍会访问东方财富。东方财富断连/验证时，
    # 直接换到腾讯行情源枚举主板代码，避免两个入口同时卡死在同一数据源。
    try:
        rows = fetch_candidate_table_via_tencent(min_market_cap_yi)
        if rows:
            return rows
        errors.append("腾讯行情候选表返回为空")
    except Exception as exc:
        errors.append(f"腾讯行情候选表获取失败: {exc}")

    try:
        rows = fetch_candidate_table_via_eastmoney(min_market_cap_yi)
        if rows:
            return rows
        errors.append("东方财富候选表返回为空")
    except Exception as exc:
        errors.append(f"东方财富候选表获取失败: {exc}")

    raise RuntimeError(
        "候选表获取失败。已依次尝试 AkShare、腾讯行情、东方财富。"
        "如果网络仍失败，可先使用已有 data/candidate_table.csv 缓存。详细错误: "
        + " | ".join(errors)
    )


def fetch_candidate_table_via_eastmoney(min_market_cap_yi: float) -> list[StockCandidate]:
    session = create_session()
    page = 1
    page_size = 100
    candidates: dict[str, StockCandidate] = {}

    while True:
        params = {
            "pn": page,
            "pz": page_size,
            "po": 1,
            "np": 1,
            "fltt": 2,
            "invt": 2,
            "fid": "f20",
            "fs": "m:1+t:2,m:0+t:6,m:0+t:80",
            "fields": "f12,f14,f20,f2,f26,f100",
            "ut": EASTMONEY_QUOTE_UT,
        }
        payload = request_json_from_any(session, EASTMONEY_LIST_URLS, params=params)
        data = payload.get("data") or {}
        rows = data.get("diff") or []
        if not rows:
            break

        for row in rows:
            code = str(row.get("f12") or "").strip().zfill(6)
            name = str(row.get("f14") or "").strip()
            total_market_cap_yi = normalize_market_cap_yi(row.get("f20"), "总市值")
            if not code or not name or total_market_cap_yi is None:
                continue
            if not is_main_board_code(code) or not is_non_st_name(name):
                continue
            if total_market_cap_yi <= min_market_cap_yi:
                continue
            candidates[code] = StockCandidate(
                code=code,
                name=name,
                market=market_name(code),
                secid=secid_for_code(code),
                total_market_cap_yi=total_market_cap_yi,
                latest_price=to_float(row.get("f2")),
                latest_trade_date=format_eastmoney_date(row.get("f26")),
                industry=str(row.get("f100") or "").strip(),
                concepts="",
            )

        total = int(data.get("total") or 0)
        if page * page_size >= total:
            break
        page += 1
        time.sleep(0.2)

    return sorted(candidates.values(), key=lambda item: item.total_market_cap_yi, reverse=True)


def fetch_candidate_table_via_akshare(min_market_cap_yi: float) -> list[StockCandidate]:
    df = ak.stock_zh_a_spot_em()
    if df is None or df.empty:
        return []

    code_col = pick_dataframe_column(df, ("代码", "股票代码", "symbol"))
    name_col = pick_dataframe_column(df, ("名称", "股票名称", "name"))
    price_col = pick_dataframe_column(df, ("最新价", "最新", "价格", "latest"))
    industry_col = pick_dataframe_column(df, ("所属行业", "行业", "industry"))
    market_cap_col = pick_dataframe_column(df, ("总市值", "总市值(亿元)", "总市值（亿元）"))
    if not code_col or not name_col or not market_cap_col:
        raise RuntimeError(f"AkShare 返回结果缺少必要列，实际列: {list(df.columns)}")

    latest_trade_date = f"{dt.datetime.now():%Y-%m-%d}"
    candidates: dict[str, StockCandidate] = {}
    for _, row in df.iterrows():
        code = str(row.get(code_col, "")).strip().zfill(6)
        name = str(row.get(name_col, "")).strip()
        if not code or not name:
            continue
        if not is_main_board_code(code) or not is_non_st_name(name):
            continue

        total_market_cap_yi = normalize_market_cap_yi(row.get(market_cap_col), market_cap_col)
        if total_market_cap_yi is None or total_market_cap_yi <= min_market_cap_yi:
            continue

        candidates[code] = StockCandidate(
            code=code,
            name=name,
            market=market_name(code),
            secid=secid_for_code(code),
            total_market_cap_yi=total_market_cap_yi,
            latest_price=to_float(row.get(price_col)) if price_col else None,
            latest_trade_date=latest_trade_date,
            industry=str(row.get(industry_col) or "").strip() if industry_col else "",
            concepts="",
        )

    return sorted(candidates.values(), key=lambda item: item.total_market_cap_yi, reverse=True)


def fetch_candidate_table_via_tencent(min_market_cap_yi: float) -> list[StockCandidate]:
    session = create_session()
    session.headers.update(
        {
            "Referer": "https://gu.qq.com/",
            "Origin": "https://gu.qq.com",
            "Accept": "*/*",
        }
    )
    candidates: dict[str, StockCandidate] = {}
    codes = generate_main_board_codes()

    for start in range(0, len(codes), TENCENT_BATCH_SIZE):
        batch_codes = codes[start : start + TENCENT_BATCH_SIZE]
        symbols = ",".join(tencent_symbol_for_code(code) for code in batch_codes)
        url = TENCENT_QUOTE_URL + symbols
        response = None
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            response.encoding = "gbk"
            text = response.text
        finally:
            if response is not None:
                response.close()

        for quote_text in re.findall(r'v_[^=]+="(.*?)";', text, flags=re.S):
            stock = parse_tencent_quote(quote_text, min_market_cap_yi)
            if stock is not None:
                candidates[stock.code] = stock
        time.sleep(0.05)

    return sorted(candidates.values(), key=lambda item: item.total_market_cap_yi, reverse=True)


def generate_main_board_codes() -> list[str]:
    prefixes = ("000", "001", "002", "003", "600", "601", "603", "605")
    return [f"{prefix}{suffix:03d}" for prefix in prefixes for suffix in range(1000)]


def tencent_symbol_for_code(code: str) -> str:
    return ("sh" if code.startswith("6") else "sz") + code


def parse_tencent_quote(quote_text: str, min_market_cap_yi: float) -> StockCandidate | None:
    if not quote_text:
        return None
    parts = quote_text.split("~")
    if len(parts) < 46:
        return None

    code = (parts[2] if len(parts) > 2 else "").strip().zfill(6)
    name = (parts[1] if len(parts) > 1 else "").strip()
    if not code or not name:
        return None
    if not is_main_board_code(code) or not is_non_st_name(name):
        return None

    # 腾讯完整行情字段通常以 ~ 分隔，下标 45 为总市值，单位通常为亿元；
    # 个别情况下该字段为空，则尝试相邻字段兜底，但只接受合理的亿元数量级。
    total_market_cap_yi = None
    for index in (45, 46, 44):
        if len(parts) <= index:
            continue
        value = normalize_market_cap_yi(parts[index], "总市值(亿元)")
        if value is not None and value > min_market_cap_yi:
            total_market_cap_yi = value
            break
    if total_market_cap_yi is None or total_market_cap_yi <= min_market_cap_yi:
        return None

    latest_trade_date = ""
    if len(parts) > 30 and len(parts[30]) >= 8 and parts[30][:8].isdigit():
        latest_trade_date = f"{parts[30][:4]}-{parts[30][4:6]}-{parts[30][6:8]}"

    return StockCandidate(
        code=code,
        name=name,
        market=market_name(code),
        secid=secid_for_code(code),
        total_market_cap_yi=total_market_cap_yi,
        latest_price=to_float(parts[3]) if len(parts) > 3 else None,
        latest_trade_date=latest_trade_date or f"{dt.datetime.now():%Y-%m-%d}",
        industry="",
        concepts="",
    )


def request_json_from_any(
    session: requests.Session,
    urls: tuple[str, ...],
    params: dict[str, Any],
    retries: int = 3,
    timeout: int = 20,
) -> dict[str, Any]:
    errors: list[str] = []
    for url in urls:
        try:
            return request_json(session, url, params=params, retries=retries, timeout=timeout)
        except Exception as exc:
            errors.append(f"{url}: {exc}")
    raise RuntimeError("所有候选表接口均请求失败: " + " | ".join(errors))


def pick_dataframe_column(df: Any, candidates: tuple[str, ...]) -> str | None:
    for name in candidates:
        if name in getattr(df, "columns", []):
            return name
    return None


def read_candidate_table(path: Path) -> list[StockCandidate]:
    if not path.exists():
        return []
    rows: list[StockCandidate] = []
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for row in reader:
            code = (row.get("股票代码") or "").strip()
            name = (row.get("股票名称") or "").strip()
            total_market_cap_yi = to_float(row.get("总市值(亿元)"))
            if not code or not name or total_market_cap_yi is None:
                continue
            rows.append(
                StockCandidate(
                    code=code,
                    name=name,
                    market=(row.get("市场") or market_name(code)).strip(),
                    secid=(row.get("secid") or secid_for_code(code)).strip(),
                    total_market_cap_yi=total_market_cap_yi,
                    latest_price=to_float(row.get("最新价")),
                    latest_trade_date=(row.get("行情日期") or "").strip(),
                    industry=(row.get("所属行业") or "").strip(),
                    concepts=(row.get("相关概念") or "").strip(),
                )
            )
    return rows


def write_candidate_table(path: Path, rows: list[StockCandidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "股票代码",
                "股票名称",
                "市场",
                "所属行业",
                "相关概念",
                "总市值(亿元)",
                "最新价",
                "行情日期",
                "secid",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(
                {
                    "股票代码": row.code,
                    "股票名称": row.name,
                    "市场": row.market,
                    "所属行业": row.industry,
                    "相关概念": row.concepts,
                    "总市值(亿元)": format_number(row.total_market_cap_yi, 2),
                    "最新价": format_number(row.latest_price, 3),
                    "行情日期": row.latest_trade_date,
                    "secid": row.secid,
                }
            )


def candidate_table_is_fresh(path: Path, max_age_hours: int = 20) -> bool:
    if not path.exists():
        return False
    modified_at = dt.datetime.fromtimestamp(path.stat().st_mtime)
    return dt.datetime.now() - modified_at <= dt.timedelta(hours=max_age_hours)


def load_or_refresh_candidates(args: argparse.Namespace) -> list[StockCandidate]:
    if not args.refresh_candidates and candidate_table_is_fresh(args.candidate_output_path):
        rows = read_candidate_table(args.candidate_output_path)
        if rows:
            print(f"复用候选表 {args.candidate_output_path}，共 {len(rows)} 只。")
            return rows

    print("正在刷新候选表：沪深主板 10cm、非 ST、总市值大于阈值...")
    try:
        rows = fetch_candidate_table(args.min_market_cap_yi)
    except Exception as exc:
        cached_rows = read_candidate_table(args.candidate_output_path)
        if cached_rows:
            print(
                f"[WARN] 候选表刷新失败，改用本地旧缓存 {args.candidate_output_path}，"
                f"共 {len(cached_rows)} 只。失败原因: {exc}",
                file=sys.stderr,
            )
            return cached_rows
        raise RuntimeError(
            "候选表刷新失败，且没有可复用的本地缓存。"
            "请先安装/升级 AkShare，或检查当前网络能否访问东方财富。"
            f"原始错误: {exc}"
        ) from exc

    write_candidate_table(args.candidate_output_path, rows)
    print(f"候选表已保存 {args.candidate_output_path}，共 {len(rows)} 只。")
    return rows


def kline_path(kline_dir: Path, code: str) -> Path:
    return kline_dir / f"{code}.csv"


def read_kline_csv(path: Path) -> list[KlineRow]:
    if not path.exists():
        return []
    rows: list[KlineRow] = []
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                rows.append(
                    KlineRow(
                        trade_date=dt.date.fromisoformat(row["交易日期"]),
                        open_price=float(row["开盘价"]),
                        close_price=float(row["收盘价"]),
                        high_price=float(row["最高价"]),
                        low_price=float(row["最低价"]),
                        volume=int(float(row["成交量"])),
                        amount=float(row["成交额"]),
                        amplitude_pct=to_float(row.get("振幅(%)")),
                        pct_change=to_float(row.get("涨跌幅(%)")),
                        change_amount=to_float(row.get("涨跌额")),
                        turnover_rate_pct=to_float(row.get("换手率(%)")),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
    return sorted(rows, key=lambda item: item.trade_date)


def write_kline_csv(path: Path, rows: list[KlineRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=kline_fieldnames())
        writer.writeheader()
        for row in sorted(rows, key=lambda item: item.trade_date):
            writer.writerow(kline_to_dict(row))


def kline_fieldnames() -> list[str]:
    return [
        "交易日期",
        "开盘价",
        "收盘价",
        "最高价",
        "最低价",
        "成交量",
        "成交额",
        "成交量颜色",
        "振幅(%)",
        "涨跌幅(%)",
        "涨跌额",
        "换手率(%)",
    ]


def kline_to_dict(row: KlineRow) -> dict[str, str | int]:
    return {
        "交易日期": f"{row.trade_date:%Y-%m-%d}",
        "开盘价": format_number(row.open_price, 3),
        "收盘价": format_number(row.close_price, 3),
        "最高价": format_number(row.high_price, 3),
        "最低价": format_number(row.low_price, 3),
        "成交量": row.volume,
        "成交额": format_number(row.amount, 2),
        "成交量颜色": row.volume_color,
        "振幅(%)": format_number(row.amplitude_pct, 3),
        "涨跌幅(%)": format_number(row.pct_change, 3),
        "涨跌额": format_number(row.change_amount, 3),
        "换手率(%)": format_number(row.turnover_rate_pct, 3),
    }


def fetch_kline(
    session: requests.Session,
    stock: StockCandidate,
    begin_date: dt.date,
    end_date: dt.date,
) -> list[KlineRow]:
    params = {
        "secid": stock.secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": f"{begin_date:%Y%m%d}",
        "end": f"{end_date:%Y%m%d}",
        "lmt": str(DEFAULT_MAX_FULL_HISTORY_BARS),
        "ut": EASTMONEY_QUOTE_UT,
    }
    errors: list[str] = []
    try:
        payload = request_json(session, EASTMONEY_KLINE_URL, params=params, timeout=20)
        klines = ((payload.get("data") or {}).get("klines")) or []
        rows: list[KlineRow] = []
        for item in klines:
            row = parse_kline_item(item)
            if row:
                rows.append(row)
        if rows:
            return rows
        errors.append("东方财富日线返回为空")
    except Exception as exc:
        errors.append(f"东方财富日线失败: {exc}")

    try:
        rows = fetch_kline_via_sina(stock, begin_date, end_date)
        if rows:
            return rows
        errors.append("新浪日线返回为空")
    except Exception as exc:
        errors.append(f"新浪日线失败: {exc}")

    if ak is not None:
        try:
            rows = fetch_kline_via_akshare(stock, begin_date, end_date)
            if rows:
                return rows
            errors.append("AkShare 日线返回为空")
        except Exception as ak_exc:
            errors.append(f"AkShare 日线失败: {ak_exc}")
    else:
        errors.append("当前环境未安装 AkShare")

    raise RuntimeError("; ".join(errors))


def fetch_kline_via_sina(
    stock: StockCandidate,
    begin_date: dt.date,
    end_date: dt.date,
) -> list[KlineRow]:
    session = create_session()
    session.headers.update(
        {
            "Referer": "https://finance.sina.com.cn/",
            "Origin": "https://finance.sina.com.cn",
            "Accept": "*/*",
        }
    )
    response = None
    try:
        response = session.get(
            SINA_KLINE_URL,
            params={
                "symbol": tencent_symbol_for_code(stock.code),
                "scale": "240",
                "ma": "no",
                "datalen": str(DEFAULT_MAX_FULL_HISTORY_BARS),
            },
            timeout=15,
        )
        response.raise_for_status()
        text = response.text.strip()
    finally:
        if response is not None:
            response.close()

    match = re.search(r"\((\[.*\])\)\s*;?\s*$", text, flags=re.S)
    if not match:
        match = re.search(r"=\s*(\[.*\])\s*;?\s*$", text, flags=re.S)
    if not match:
        raise RuntimeError("新浪日线返回格式无法解析")

    items = json.loads(match.group(1))
    rows: list[KlineRow] = []
    previous_close: float | None = None
    for item in items:
        try:
            trade_date = dt.date.fromisoformat(str(item.get("day"))[:10])
            if trade_date < begin_date or trade_date > end_date:
                continue
            open_price = float(item.get("open"))
            close_price = float(item.get("close"))
            high_price = float(item.get("high"))
            low_price = float(item.get("low"))
            volume = int(float(item.get("volume") or 0))
            change_amount = None
            pct_change = None
            if previous_close and previous_close > 0:
                change_amount = close_price - previous_close
                pct_change = change_amount / previous_close * 100
            amplitude_pct = None
            if previous_close and previous_close > 0:
                amplitude_pct = (high_price - low_price) / previous_close * 100
            rows.append(
                KlineRow(
                    trade_date=trade_date,
                    open_price=open_price,
                    close_price=close_price,
                    high_price=high_price,
                    low_price=low_price,
                    volume=volume,
                    amount=0.0,
                    amplitude_pct=amplitude_pct,
                    pct_change=pct_change,
                    change_amount=change_amount,
                    turnover_rate_pct=None,
                )
            )
            previous_close = close_price
        except (TypeError, ValueError, json.JSONDecodeError):
            continue

    return sorted(rows, key=lambda item: item.trade_date)


def fetch_kline_via_akshare(
    stock: StockCandidate,
    begin_date: dt.date,
    end_date: dt.date,
) -> list[KlineRow]:
    df = ak.stock_zh_a_hist(
        symbol=stock.code,
        period="daily",
        start_date=f"{begin_date:%Y%m%d}",
        end_date=f"{end_date:%Y%m%d}",
        adjust="qfq",
    )
    if df is None or df.empty:
        return []

    date_col = pick_dataframe_column(df, ("日期", "date"))
    open_col = pick_dataframe_column(df, ("开盘", "开盘价", "open"))
    close_col = pick_dataframe_column(df, ("收盘", "收盘价", "close"))
    high_col = pick_dataframe_column(df, ("最高", "最高价", "high"))
    low_col = pick_dataframe_column(df, ("最低", "最低价", "low"))
    volume_col = pick_dataframe_column(df, ("成交量", "volume"))
    amount_col = pick_dataframe_column(df, ("成交额", "amount"))
    amplitude_col = pick_dataframe_column(df, ("振幅", "振幅(%)"))
    pct_col = pick_dataframe_column(df, ("涨跌幅", "涨跌幅(%)"))
    change_col = pick_dataframe_column(df, ("涨跌额",))
    turnover_col = pick_dataframe_column(df, ("换手率", "换手率(%)"))

    required_columns = [date_col, open_col, close_col, high_col, low_col, volume_col, amount_col]
    if any(column is None for column in required_columns):
        raise RuntimeError(f"AkShare 日线返回结果缺少必要列，实际列: {list(df.columns)}")

    rows: list[KlineRow] = []
    for _, row in df.iterrows():
        try:
            rows.append(
                KlineRow(
                    trade_date=dt.date.fromisoformat(str(row.get(date_col))[:10]),
                    open_price=float(row.get(open_col)),
                    close_price=float(row.get(close_col)),
                    high_price=float(row.get(high_col)),
                    low_price=float(row.get(low_col)),
                    volume=int(float(row.get(volume_col))),
                    amount=float(row.get(amount_col)),
                    amplitude_pct=to_float(row.get(amplitude_col)) if amplitude_col else None,
                    pct_change=to_float(row.get(pct_col)) if pct_col else None,
                    change_amount=to_float(row.get(change_col)) if change_col else None,
                    turnover_rate_pct=to_float(row.get(turnover_col)) if turnover_col else None,
                )
            )
        except (TypeError, ValueError):
            continue

    return sorted(rows, key=lambda item: item.trade_date)


def parse_kline_item(item: str) -> KlineRow | None:
    parts = item.split(",")
    if len(parts) < 11:
        return None
    try:
        return KlineRow(
            trade_date=dt.date.fromisoformat(parts[0]),
            open_price=float(parts[1]),
            close_price=float(parts[2]),
            high_price=float(parts[3]),
            low_price=float(parts[4]),
            volume=int(float(parts[5])),
            amount=float(parts[6]),
            amplitude_pct=to_float(parts[7]),
            pct_change=to_float(parts[8]),
            change_amount=to_float(parts[9]),
            turnover_rate_pct=to_float(parts[10]),
        )
    except ValueError:
        return None


def merge_klines(existing_rows: list[KlineRow], fetched_rows: list[KlineRow]) -> list[KlineRow]:
    by_date = {row.trade_date: row for row in existing_rows}
    for row in fetched_rows:
        by_date[row.trade_date] = row
    return sorted(by_date.values(), key=lambda item: item.trade_date)


def normalize_volume_units(rows: list[KlineRow]) -> list[KlineRow]:
    """Detect and fix volume unit inconsistency (shares vs lots) in merged kline data.

    Different data sources may report volume in different units:
    - Sina: shares (股), amount=0 (not provided)
    - Eastmoney: lots (手, 1 lot = 100 shares), amount>0

    Strategy:
    1. Classify each row's unit using amount/(volume*close) ratio
    2. For unknown rows (amount=0), segment by boundaries and infer from neighbors
    3. Convert all rows to the majority unit
    """
    if len(rows) < 2:
        return rows

    VOLUME_JUMP_THRESHOLD = 50.0
    LOT_RATIO_THRESHOLD = 50.0

    # Step 1: Classify rows with amount data
    # unit: 1 = shares (股), 100 = lots (手), None = unknown
    row_unit: list[float | None] = [None] * len(rows)
    for i, row in enumerate(rows):
        if row.amount > 0 and row.volume > 0 and row.close_price > 0:
            ratio = row.amount / (row.volume * row.close_price)
            row_unit[i] = 100.0 if ratio > LOT_RATIO_THRESHOLD else 1.0

    # Step 2: Find unit boundaries (>50x volume jumps)
    boundaries: list[int] = []
    for i in range(1, len(rows)):
        prev_vol = rows[i - 1].volume
        curr_vol = rows[i].volume
        if prev_vol > 0 and curr_vol > 0:
            ratio = max(prev_vol, curr_vol) / min(prev_vol, curr_vol)
            if ratio > VOLUME_JUMP_THRESHOLD:
                boundaries.append(i)

    if not boundaries:
        # No boundaries found — check if any rows have known units
        known = [u for u in row_unit if u is not None]
        if not known or all(u == known[0] for u in known):
            return rows

    # Step 3: Create segments split by boundaries, infer unknown units
    # Segments: [0, b1), [b1, b2), ..., [bn, len(rows))
    segment_starts = [0] + boundaries
    segment_ends = boundaries + [len(rows)]

    for seg_idx in range(len(segment_starts)):
        seg_start = segment_starts[seg_idx]
        seg_end = segment_ends[seg_idx]

        # Find known units in this segment
        seg_units = [row_unit[i] for i in range(seg_start, seg_end) if row_unit[i] is not None]

        if seg_units:
            # Use the majority unit within this segment
            majority_unit = 100.0 if seg_units.count(100.0) > seg_units.count(1.0) else 1.0
        else:
            # All unknown — check adjacent segments for hints
            majority_unit = None
            # Look at the next segment's known units
            for other_idx in range(seg_idx + 1, len(segment_starts)):
                other_start = segment_starts[other_idx]
                other_end = segment_ends[other_idx]
                other_units = [row_unit[i] for i in range(other_start, other_end) if row_unit[i] is not None]
                if other_units:
                    # If next segment is lots, this segment (older) is likely shares
                    next_unit = 100.0 if other_units.count(100.0) > other_units.count(1.0) else 1.0
                    majority_unit = 1.0 if next_unit == 100.0 else 100.0
                    break
            if majority_unit is None:
                # Look at the previous segment
                for other_idx in range(seg_idx - 1, -1, -1):
                    other_start = segment_starts[other_idx]
                    other_end = segment_ends[other_idx]
                    other_units = [row_unit[i] for i in range(other_start, other_end) if row_unit[i] is not None]
                    if other_units:
                        prev_unit = 100.0 if other_units.count(100.0) > other_units.count(1.0) else 1.0
                        majority_unit = 1.0 if prev_unit == 100.0 else 100.0
                        break
            if majority_unit is None:
                continue  # Can't determine, skip segment

        # Assign inferred unit to unknown rows in this segment
        for i in range(seg_start, seg_end):
            if row_unit[i] is None:
                row_unit[i] = majority_unit

    # Step 4: Determine the global majority unit and convert minority rows
    known_units = [u for u in row_unit if u is not None]
    if not known_units:
        return rows

    shares_count = known_units.count(1.0)
    lots_count = known_units.count(100.0)

    if lots_count == 0 or shares_count == 0:
        return rows  # All same unit, nothing to do

    # Convert minority to majority
    if shares_count >= lots_count:
        target_unit = 1.0  # shares
        convert_unit = 100.0
        multiplier = 100  # lots → shares
    else:
        target_unit = 100.0  # lots
        convert_unit = 1.0
        multiplier = 0.01  # shares → lots

    convert_indices = [i for i in range(len(rows)) if row_unit[i] == convert_unit]
    if not convert_indices:
        return rows

    new_rows: list[KlineRow] = list(rows)
    for idx in convert_indices:
        old = rows[idx]
        new_volume = int(old.volume * multiplier)
        new_rows[idx] = KlineRow(
            trade_date=old.trade_date,
            open_price=old.open_price,
            close_price=old.close_price,
            high_price=old.high_price,
            low_price=old.low_price,
            volume=new_volume,
            amount=old.amount,
            amplitude_pct=old.amplitude_pct,
            pct_change=old.pct_change,
            change_amount=old.change_amount,
            turnover_rate_pct=old.turnover_rate_pct,
        )

    print(f"  成交量单位归一化: 转换 {len(convert_indices)}/{len(rows)} 行成交量。")
    return new_rows


def trim_kline_rows(rows: list[KlineRow], earliest_date: dt.date) -> list[KlineRow]:
    return [row for row in rows if row.trade_date >= earliest_date]


def update_one_kline(
    stock: StockCandidate,
    kline_dir: Path,
    history_begin: dt.date,
    effective_end_date: dt.date,
    force: bool,
) -> tuple[str, int, str]:
    path = kline_path(kline_dir, stock.code)
    existing_rows = [] if force else read_kline_csv(path)

    if existing_rows:
        last_date = existing_rows[-1].trade_date
        fetch_begin = max(history_begin, last_date - dt.timedelta(days=10))
    else:
        fetch_begin = history_begin

    if not force and existing_rows and existing_rows[-1].trade_date >= effective_end_date:
        trimmed_rows = trim_kline_rows(existing_rows, history_begin)
        if len(trimmed_rows) != len(existing_rows):
            write_kline_csv(path, trimmed_rows)
        return stock.code, len(trimmed_rows), "cached"

    session = create_session()
    fetched_rows = fetch_kline(session, stock, fetch_begin, effective_end_date)
    merged_rows = merge_klines(existing_rows, fetched_rows)
    merged_rows = normalize_volume_units(merged_rows)
    trimmed_rows = trim_kline_rows(merged_rows, history_begin)
    write_kline_csv(path, trimmed_rows)
    return stock.code, len(trimmed_rows), "updated"


def renormalize_all_klines(kline_dir: Path) -> None:
    """Force renormalize volume units for all existing kline CSVs."""
    csv_files = sorted(kline_dir.glob("*.csv"))
    if not csv_files:
        print("没有找到本地K线缓存，跳过归一化。")
        return

    print(f"正在对 {len(csv_files)} 只股票的K线做成交量单位归一化...")
    fixed = 0
    for path in csv_files:
        rows = read_kline_csv(path)
        if not rows:
            continue
        normalized = normalize_volume_units(rows)
        if normalized is not rows:
            write_kline_csv(path, normalized)
            fixed += 1
    print(f"归一化完成: {fixed}/{len(csv_files)} 只股票的K线被修改。")


def update_kline_cache(args: argparse.Namespace, candidates: list[StockCandidate]) -> None:
    if args.skip_kline_update:
        print("已跳过联网更新日线，只使用本地缓存。")
        return

    history_begin = args.effective_end_date - dt.timedelta(days=args.history_days + 14)
    args.kline_dir_path.mkdir(parents=True, exist_ok=True)
    total = len(candidates)
    updated = 0
    cached = 0
    failed = 0

    print(f"正在增量更新日线：{total} 只，目标截止 {args.effective_end_date:%Y-%m-%d}...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                update_one_kline,
                stock,
                args.kline_dir_path,
                history_begin,
                args.effective_end_date,
                args.force_kline_update,
            ): stock
            for stock in candidates
        }
        for index, future in enumerate(as_completed(futures), start=1):
            stock = futures[future]
            try:
                _code, _row_count, status = future.result()
                if status == "updated":
                    updated += 1
                else:
                    cached += 1
            except Exception as exc:
                failed += 1
                print(f"[WARN] {stock.code} {stock.name} 日线更新失败: {exc}", file=sys.stderr)
            if index % 50 == 0 or index == total:
                print(f"日线进度 {index}/{total}，更新 {updated}，复用 {cached}，失败 {failed}。")


def _screen_one_stock(
    stock: StockCandidate,
    args: argparse.Namespace,
) -> ScreenResult | None:
    """对单只股票执行低量下跌日筛选，命中返回 ScreenResult，否则返回 None。"""
    rows = read_kline_csv(kline_path(args.kline_dir_path, stock.code))
    rows = [row for row in rows if row.trade_date <= args.effective_end_date]
    if not rows:
        return None

    lookback_rows = rows[-args.lookback_days :]
    if len(lookback_rows) < args.lookback_days:
        return None

    recent_rows = lookback_rows[-args.recent_days :]
    if len(recent_rows) < args.recent_days:
        return None

    latest_row = recent_rows[-1]
    pct_by_date = build_pct_change_by_date(rows)

    theme_score, theme_tags = score_theme_relevance(stock, args)
    if args.require_theme and theme_score <= 0:
        return None
    if args.min_theme_score > 0 and theme_score < args.min_theme_score:
        return None

    # 1) 流动性过滤：市值大但成交额太低的票，容易出现“无人问津式地量”。
    latest_20_rows = lookback_rows[-min(20, len(lookback_rows)) :]
    avg_amount_20d_yi = average([row.amount for row in latest_20_rows]) / 100_000_000
    if args.min_avg_amount_yi > 0 and avg_amount_20d_yi < args.min_avg_amount_yi:
        return None

    # 2) 破位过滤：低量下跌如果伴随深度破位，通常不是低吸，而是趋势走坏。
    high_20 = max(row.high_price for row in latest_20_rows)
    drop_20d_pct = (latest_row.close_price / high_20 - 1) * 100 if high_20 > 0 else None
    if (
        args.max_20d_drop_pct > 0
        and drop_20d_pct is not None
        and drop_20d_pct < -args.max_20d_drop_pct
    ):
        return None

    latest_60_rows = lookback_rows[-min(60, len(lookback_rows)) :]
    low_60 = min(row.low_price for row in latest_60_rows)
    distance_from_60d_low_pct = (
        (latest_row.close_price / low_60 - 1) * 100 if low_60 > 0 else None
    )
    if (
        args.min_distance_from_60d_low_pct > 0
        and distance_from_60d_low_pct is not None
        and distance_from_60d_low_pct < args.min_distance_from_60d_low_pct
    ):
        return None

    # 3) 整体缩量过滤：要求近期均量低于中期均量，避免只因单日偶然低量入选。
    latest_5_rows = lookback_rows[-min(5, len(lookback_rows)) :]
    avg_volume_5 = average([row.volume for row in latest_5_rows])
    avg_volume_60 = average([row.volume for row in latest_60_rows])
    recent_volume_ratio = avg_volume_5 / avg_volume_60 if avg_volume_60 > 0 else None
    if (
        args.recent_volume_shrink_ratio > 0
        and recent_volume_ratio is not None
        and recent_volume_ratio > args.recent_volume_shrink_ratio
    ):
        return None

    # 4) 均线确认：用于避免还在连续阴跌时过早低吸。
    ma5 = moving_average(lookback_rows, 5)
    ma10 = moving_average(lookback_rows, 10)
    ma20 = moving_average(lookback_rows, 20)
    ma60 = moving_average(lookback_rows, 60)
    price_above_ma5 = ma5 is not None and latest_row.close_price >= ma5
    price_above_ma10 = ma10 is not None and latest_row.close_price >= ma10
    price_above_ma20 = ma20 is not None and latest_row.close_price >= ma20
    if args.require_price_above_ma5 and not price_above_ma5:
        return None
    if args.require_price_above_ma10 and not price_above_ma10:
        return None
    if args.require_price_above_ma20 and not price_above_ma20:
        return None

    limit_dates: set[dt.date] = set()
    if not args.keep_limit_days:
        limit_dates = find_limit_price_dates(lookback_rows, args.limit_pct_threshold)

    rankable_rows = [row for row in lookback_rows if row.trade_date not in limit_dates]

    # 最新成交量诊断：把最新一个交易日的成交量，与回看周期内所有有效交易日成交量一起排序。
    # 分位越低，说明当前成交手数越接近历史地量。
    all_volume_rank_by_date, all_volume_percentile_by_date = build_volume_rank_maps(rankable_rows)
    latest_volume_rank = all_volume_rank_by_date.get(latest_row.trade_date)
    latest_volume_percentile = all_volume_percentile_by_date.get(latest_row.trade_date)
    if (
        args.max_latest_volume_percentile > 0
        and (latest_volume_percentile is None or latest_volume_percentile > args.max_latest_volume_percentile)
    ):
        return None

    down_rows = [
        row
        for row in rankable_rows
        if is_down_day(row, pct_by_date.get(row.trade_date), args.down_day_mode)
    ]
    if not down_rows:
        return None

    down_sorted = sorted(down_rows, key=lambda item: (item.volume, item.trade_date))
    cutoff_count = max(1, math.ceil(len(down_sorted) * args.lowest_green_percent / 100))
    cutoff_rows = down_sorted[:cutoff_count]
    cutoff_volume = max(row.volume for row in cutoff_rows)

    rank_by_date: dict[dt.date, int] = {}
    percentile_by_date: dict[dt.date, float] = {}
    for rank, row in enumerate(down_sorted, start=1):
        rank_by_date[row.trade_date] = rank
        percentile_by_date[row.trade_date] = rank / len(down_sorted) * 100

    latest_is_down = is_down_day(latest_row, pct_by_date.get(latest_row.trade_date), args.down_day_mode)
    latest_down_volume_rank = rank_by_date.get(latest_row.trade_date)
    latest_down_volume_percentile = percentile_by_date.get(latest_row.trade_date)
    latest_volume_to_cutoff_ratio = (latest_row.volume / cutoff_volume) if cutoff_volume > 0 else None

    hit_rows = [
        row
        for row in recent_rows
        if row.trade_date not in limit_dates
        and is_down_day(row, pct_by_date.get(row.trade_date), args.down_day_mode)
        and row.volume <= cutoff_volume
    ]
    if len(hit_rows) < args.min_hit_days:
        return None

    confirm_signal = describe_confirm_signal(lookback_rows, hit_rows, pct_by_date)
    if args.require_confirm_signal and confirm_signal == "无":
        return None

    latest_hit = max(hit_rows, key=lambda item: item.trade_date)
    latest_hit_low_price = latest_hit.low_price
    not_break_low_volume_low = latest_row.close_price >= latest_hit_low_price
    if args.require_not_break_low_volume_low and not not_break_low_volume_low:
        return None

    stop_price = latest_hit_low_price * 0.98 if latest_hit_low_price > 0 else None
    high_60 = max(row.high_price for row in latest_60_rows) if latest_60_rows else None
    target_price = high_60
    reward_risk_ratio = calc_reward_risk_ratio(latest_row.close_price, stop_price, target_price)
    if (
        args.min_reward_risk_ratio > 0
        and (reward_risk_ratio is None or reward_risk_ratio < args.min_reward_risk_ratio)
    ):
        return None

    low_absorb_score, score_detail = score_low_absorb(
        hit_down_percentiles=[percentile_by_date[row.trade_date] for row in hit_rows],
        latest_volume_percentile=latest_volume_percentile,
        latest_down_volume_percentile=latest_down_volume_percentile,
        recent_volume_ratio=recent_volume_ratio,
        drop_20d_pct=drop_20d_pct,
        distance_from_60d_low_pct=distance_from_60d_low_pct,
        confirm_signal=confirm_signal,
        latest_close=latest_row.close_price,
        ma5=ma5,
        ma10=ma10,
        ma20=ma20,
        ma60=ma60,
        reward_risk_ratio=reward_risk_ratio,
        not_break_low_volume_low=not_break_low_volume_low,
    )
    if args.min_low_absorb_score > 0 and low_absorb_score < args.min_low_absorb_score:
        return None

    final_score = combine_final_score(low_absorb_score, theme_score, args.theme_score_weight)
    if args.min_final_score > 0 and final_score < args.min_final_score:
        return None

    suggested_status = classify_low_absorb_status(
        low_absorb_score=low_absorb_score,
        confirm_signal=confirm_signal,
        not_break_low_volume_low=not_break_low_volume_low,
        price_above_ma5=price_above_ma5,
        price_above_ma10=price_above_ma10,
        reward_risk_ratio=reward_risk_ratio,
    )

    return ScreenResult(
        code=stock.code,
        name=stock.name,
        market=stock.market,
        industry=stock.industry,
        concepts=stock.concepts,
        theme_tags=theme_tags,
        theme_score=theme_score,
        total_market_cap_yi=stock.total_market_cap_yi,
        recent_window_days=len(recent_rows),
        hit_days=len(hit_rows),
        hit_dates=[f"{row.trade_date:%Y-%m-%d}" for row in hit_rows],
        hit_green_volumes=[row.volume for row in hit_rows],
        hit_green_ranks=[rank_by_date[row.trade_date] for row in hit_rows],
        hit_green_percentiles=[percentile_by_date[row.trade_date] for row in hit_rows],
        latest_volume=latest_row.volume,
        latest_volume_rank=latest_volume_rank,
        latest_volume_percentile=latest_volume_percentile,
        latest_is_down_day=latest_is_down,
        latest_down_volume_rank=latest_down_volume_rank,
        latest_down_volume_percentile=latest_down_volume_percentile,
        latest_volume_to_cutoff_ratio=latest_volume_to_cutoff_ratio,
        green_cutoff_volume=cutoff_volume,
        green_sample_count=len(down_rows),
        excluded_limit_days=len(limit_dates),
        avg_amount_20d_yi=avg_amount_20d_yi,
        recent_volume_ratio=recent_volume_ratio,
        drop_20d_pct=drop_20d_pct,
        distance_from_60d_low_pct=distance_from_60d_low_pct,
        confirm_signal=confirm_signal,
        latest_hit_low_price=latest_hit_low_price,
        not_break_low_volume_low=not_break_low_volume_low,
        ma5=ma5,
        ma10=ma10,
        ma20=ma20,
        ma60=ma60,
        price_above_ma5=price_above_ma5,
        price_above_ma10=price_above_ma10,
        price_above_ma20=price_above_ma20,
        stop_price=stop_price,
        target_price=target_price,
        reward_risk_ratio=reward_risk_ratio,
        low_absorb_score=low_absorb_score,
        final_score=final_score,
        exhaustion_score=score_detail["卖盘枯竭评分"],
        shrink_score=score_detail["整体缩量评分"],
        position_score=score_detail["位置安全评分"],
        trend_score=score_detail["趋势质量评分"],
        confirm_score=score_detail["确认信号评分"],
        reward_risk_score=score_detail["风险收益评分"],
        suggested_status=suggested_status,
        lookback_start=f"{lookback_rows[0].trade_date:%Y-%m-%d}",
        lookback_end=f"{lookback_rows[-1].trade_date:%Y-%m-%d}",
        latest_close=latest_row.close_price,
        latest_pct_change=pct_by_date.get(latest_row.trade_date),
    )


def select_low_absorb_stocks(
    args: argparse.Namespace,
    candidates: list[StockCandidate],
) -> list[ScreenResult]:
    compute_workers = getattr(args, "compute_workers", 1)
    results: list[ScreenResult] = []
    total = len(candidates)

    if compute_workers > 1:
        with ThreadPoolExecutor(max_workers=compute_workers) as executor:
            futures = {
                executor.submit(_screen_one_stock, stock, args): stock
                for stock in candidates
            }
            for index, future in enumerate(as_completed(futures), start=1):
                stock = futures[future]
                try:
                    result = future.result()
                    if result is not None:
                        results.append(result)
                except Exception as exc:
                    print(f"[WARN] {stock.code} {stock.name} 筛选异常: {exc}", file=sys.stderr)
                if index % 100 == 0 or index == total:
                    print(f"筛选进度 {index}/{total}，命中 {len(results)} 只。")
    else:
        for index, stock in enumerate(candidates, start=1):
            try:
                result = _screen_one_stock(stock, args)
                if result is not None:
                    results.append(result)
            except Exception as exc:
                print(f"[WARN] {stock.code} {stock.name} 筛选异常: {exc}", file=sys.stderr)
            if index % 100 == 0 or index == total:
                print(f"筛选进度 {index}/{total}，命中 {len(results)} 只。")

    return sorted(
        results,
        key=lambda item: (
            -item.final_score,
            -item.low_absorb_score,
            -item.hit_days,
            max(item.hit_green_percentiles),
            -(item.reward_risk_ratio or 0),
            -item.total_market_cap_yi,
        ),
    )


def build_pct_change_by_date(rows: list[KlineRow]) -> dict[dt.date, float | None]:
    """Build a stable pct_change map and fill missing values from previous close."""
    result: dict[dt.date, float | None] = {}
    previous_row: KlineRow | None = None
    for row in rows:
        pct_change = row.pct_change
        if pct_change is None and previous_row is not None and previous_row.close_price > 0:
            pct_change = (row.close_price - previous_row.close_price) / previous_row.close_price * 100
        result[row.trade_date] = pct_change
        previous_row = row
    return result


def is_down_day(row: KlineRow, pct_change: float | None, mode: str) -> bool:
    """Return whether a row should be treated as a down day for low-volume ranking."""
    if mode == "green":
        return row.close_price < row.open_price
    if pct_change is None:
        # 只有数据源缺涨跌幅且无法补算时，才退回绿 K 口径。
        return row.close_price < row.open_price
    return pct_change < 0


def average(values: list[float | int]) -> float:
    cleaned = [float(value) for value in values if value is not None]
    if not cleaned:
        return 0.0
    return sum(cleaned) / len(cleaned)




def load_theme_map(path: Path) -> dict[str, dict[str, Any]]:
    """Load a user-maintainable hot-theme map.

    Supported columns: 股票代码, 股票名称, 主题标签, 主题评分, 备注.
    The file is optional. Missing file means no hard-code theme override.
    """
    if not path.exists():
        return {}
    result: dict[str, dict[str, Any]] = {}
    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        for row in reader:
            code = normalize_stock_code(row.get("股票代码") or row.get("code") or "")
            if not code:
                continue
            tags = split_theme_tags(row.get("主题标签") or row.get("tags") or "")
            score = to_float(row.get("主题评分") or row.get("score"))
            result[code] = {
                "name": (row.get("股票名称") or row.get("name") or "").strip(),
                "tags": tags,
                "score": clamp(score if score is not None else 80.0),
                "note": (row.get("备注") or row.get("note") or "").strip(),
            }
    return result


def normalize_stock_code(value: object) -> str:
    text = str(value or "").strip()
    match = re.search(r"(\d{6})", text)
    return match.group(1) if match else ""


def split_theme_tags(text: str) -> list[str]:
    tags: list[str] = []
    for item in re.split(r"[,，/、;；|\s]+", text or ""):
        item = item.strip()
        if item and item not in tags:
            tags.append(item)
    return tags


HOT_THEME_KEYWORDS: tuple[tuple[str, tuple[str, ...], float], ...] = (
    ("AI算力", ("算力", "AI服务器", "服务器", "数据中心", "液冷", "CPO", "光模块", "光通信", "PCB", "高速铜缆", "交换机", "电源"), 90.0),
    ("存储芯片", ("存储", "DRAM", "NAND", "HBM", "半导体", "国产芯片", "封测", "先进封装", "半导体设备"), 88.0),
    ("人形机器人", ("机器人", "人形机器人", "减速器", "伺服", "电机", "传感器", "丝杠", "控制器"), 86.0),
    ("工业母机", ("工业母机", "数控机床", "机床", "刀具", "高端装备", "通用设备", "新型工业化"), 82.0),
    ("商业航天", ("商业航天", "卫星互联网", "航天", "北斗", "军工电子", "低空经济", "大飞机", "无人机"), 84.0),
    ("国产替代", ("国产替代", "信创", "自主可控", "国产软件", "华为", "鸿蒙", "操作系统"), 78.0),
)


def score_theme_relevance(stock: StockCandidate, args: argparse.Namespace) -> tuple[float, list[str]]:
    """Score whether a stock belongs to current high-prosperity themes.

    This is deliberately a soft score. It should improve ranking and allow
    optional filtering, but it must not replace the price-volume signal.
    """
    tags: list[str] = []
    score = 0.0
    theme_map: dict[str, dict[str, Any]] = getattr(args, "theme_map", {}) or {}
    mapped = theme_map.get(stock.code)
    if mapped:
        score = max(score, float(mapped.get("score") or 80.0))
        for tag in mapped.get("tags") or []:
            if tag not in tags:
                tags.append(tag)

    haystack = " ".join([stock.name, stock.industry, stock.concepts]).upper()
    for tag, keywords, keyword_score in HOT_THEME_KEYWORDS:
        for keyword in keywords:
            if keyword.upper() in haystack:
                score = max(score, keyword_score)
                if tag not in tags:
                    tags.append(tag)
                break

    return round(clamp(score), 2), tags


def combine_final_score(low_absorb_score: float, theme_score: float, theme_weight: float) -> float:
    weight = clamp(theme_weight, 0.0, 50.0) / 100.0
    return round(low_absorb_score * (1 - weight) + theme_score * weight, 2)


def index_secid_for_code(code: str) -> str:
    code = normalize_stock_code(code)
    if code.startswith("399"):
        return f"0.{code}"
    return f"1.{code}"


def index_name_for_code(code: str) -> str:
    names = {"000001": "上证指数", "399001": "深证成指", "000300": "沪深300"}
    return names.get(normalize_stock_code(code), f"指数{code}")


def update_market_index_cache(args: argparse.Namespace) -> None:
    if args.market_filter == "off":
        return
    code = normalize_stock_code(args.market_index_code)
    if not code:
        raise RuntimeError("--market-index-code 必须包含 6 位指数代码")
    history_begin = args.effective_end_date - dt.timedelta(days=args.history_days + 14)
    index_stock = StockCandidate(
        code=code,
        name=index_name_for_code(code),
        market="指数",
        secid=index_secid_for_code(code),
        total_market_cap_yi=0.0,
        latest_price=None,
        latest_trade_date="",
        industry="指数",
        concepts="大盘环境",
    )
    args.kline_dir_path.mkdir(parents=True, exist_ok=True)
    try:
        _code, _rows, status = update_one_kline(
            index_stock,
            args.kline_dir_path,
            history_begin,
            args.effective_end_date,
            args.force_kline_update,
        )
        print(f"大盘指数缓存 {index_stock.name}({code}) {status}。")
    except Exception as exc:
        raise RuntimeError(f"大盘环境过滤需要指数日线，但 {code} 更新失败: {exc}") from exc


def market_filter_passed(args: argparse.Namespace) -> tuple[bool, str]:
    if args.market_filter == "off":
        return True, "未启用大盘过滤"
    code = normalize_stock_code(args.market_index_code)
    rows = read_kline_csv(kline_path(args.kline_dir_path, code))
    rows = [row for row in rows if row.trade_date <= args.effective_end_date]
    if len(rows) < 60:
        return False, f"{index_name_for_code(code)} 可用日线不足 60 条，无法做大盘过滤"
    latest = rows[-1]
    ma20 = moving_average(rows, 20)
    ma60 = moving_average(rows, 60)
    above20 = ma20 is not None and latest.close_price >= ma20
    above60 = ma60 is not None and latest.close_price >= ma60
    mode = args.market_filter
    if mode == "ma20":
        ok = above20
    elif mode == "ma60":
        ok = above60
    elif mode == "ma20-and-ma60":
        ok = above20 and above60
    elif mode == "ma20-or-ma60":
        ok = above20 or above60
    else:
        ok = True
    msg = (
        f"{index_name_for_code(code)} {latest.trade_date:%Y-%m-%d} 收盘 {latest.close_price:.2f}，"
        f"MA20={ma20:.2f}，MA60={ma60:.2f}，过滤模式={mode}，结果={'通过' if ok else '未通过'}"
    )
    return ok, msg

def moving_average(rows: list[KlineRow], days: int) -> float | None:
    if len(rows) < days:
        return None
    values = [row.close_price for row in rows[-days:]]
    return average(values)


def calc_reward_risk_ratio(
    latest_close: float,
    stop_price: float | None,
    target_price: float | None,
) -> float | None:
    if stop_price is None or target_price is None:
        return None
    risk = latest_close - stop_price
    reward = target_price - latest_close
    if risk <= 0 or reward <= 0:
        return None
    return reward / risk


def clamp(value: float, min_value: float = 0.0, max_value: float = 100.0) -> float:
    return max(min_value, min(max_value, value))


def score_low_absorb(
    hit_down_percentiles: list[float],
    latest_volume_percentile: float | None,
    latest_down_volume_percentile: float | None,
    recent_volume_ratio: float | None,
    drop_20d_pct: float | None,
    distance_from_60d_low_pct: float | None,
    confirm_signal: str,
    latest_close: float,
    ma5: float | None,
    ma10: float | None,
    ma20: float | None,
    ma60: float | None,
    reward_risk_ratio: float | None,
    not_break_low_volume_low: bool,
) -> tuple[float, dict[str, float]]:
    """Calculate a 0-100 low-absorb score.

    The score is not a buy signal. It ranks candidates by whether they combine:
    low-volume down days, current low turnover, non-broken structure, short-term
    trend repair, confirmation, and usable reward/risk.
    """

    # 1) 卖盘枯竭：命中低量日越靠近最低 5%，最新成交量越低，分数越高。
    if hit_down_percentiles:
        worst_hit_percentile = max(hit_down_percentiles)
        hit_score = clamp(110 - worst_hit_percentile * 6)
    else:
        hit_score = 0.0

    latest_candidates = [value for value in (latest_down_volume_percentile, latest_volume_percentile) if value is not None]
    if latest_candidates:
        latest_percentile = min(latest_candidates)
        latest_score = clamp(110 - latest_percentile * 3)
    else:
        latest_score = 40.0

    exhaustion_score = clamp(hit_score * 0.70 + latest_score * 0.30)

    # 2) 整体缩量：0.5-0.7 通常较好；过高说明没有缩量。
    if recent_volume_ratio is None:
        shrink_score = 45.0
    elif recent_volume_ratio <= 0.55:
        shrink_score = 100.0
    elif recent_volume_ratio <= 0.70:
        shrink_score = 85.0
    elif recent_volume_ratio <= 0.85:
        shrink_score = 65.0
    elif recent_volume_ratio <= 1.00:
        shrink_score = 45.0
    else:
        shrink_score = 20.0

    # 3) 位置安全：不能跌太深，也不能贴近 60 日新低；跌破地量低点重罚。
    position_score = 100.0
    if drop_20d_pct is not None:
        if drop_20d_pct < -25:
            position_score -= 55
        elif drop_20d_pct < -20:
            position_score -= 40
        elif drop_20d_pct < -15:
            position_score -= 22
        elif drop_20d_pct < -10:
            position_score -= 10
    if distance_from_60d_low_pct is not None:
        if distance_from_60d_low_pct < 2:
            position_score -= 45
        elif distance_from_60d_low_pct < 5:
            position_score -= 25
        elif distance_from_60d_low_pct < 8:
            position_score -= 10
    if not not_break_low_volume_low:
        position_score -= 45
    position_score = clamp(position_score)

    # 4) 趋势质量：越多短中期均线被收复，买盘恢复迹象越好。
    trend_score = 35.0
    if ma5 is not None and latest_close >= ma5:
        trend_score += 25
    if ma10 is not None and latest_close >= ma10:
        trend_score += 20
    if ma20 is not None and latest_close >= ma20:
        trend_score += 15
    if ma60 is not None and latest_close >= ma60:
        trend_score += 5
    trend_score = clamp(trend_score)

    # 5) 确认信号：有放量阳线突破地量日高点，则更接近买点。
    confirm_score = 100.0 if confirm_signal != "无" else 45.0

    # 6) 盈亏比：空间不足的低吸不值得做。
    if reward_risk_ratio is None:
        reward_risk_score = 35.0
    elif reward_risk_ratio >= 3:
        reward_risk_score = 100.0
    elif reward_risk_ratio >= 2:
        reward_risk_score = 82.0
    elif reward_risk_ratio >= 1.5:
        reward_risk_score = 62.0
    elif reward_risk_ratio >= 1:
        reward_risk_score = 42.0
    else:
        reward_risk_score = 18.0

    total_score = (
        exhaustion_score * 0.25
        + shrink_score * 0.15
        + position_score * 0.20
        + trend_score * 0.15
        + confirm_score * 0.15
        + reward_risk_score * 0.10
    )

    detail = {
        "卖盘枯竭评分": round(exhaustion_score, 2),
        "整体缩量评分": round(shrink_score, 2),
        "位置安全评分": round(position_score, 2),
        "趋势质量评分": round(trend_score, 2),
        "确认信号评分": round(confirm_score, 2),
        "风险收益评分": round(reward_risk_score, 2),
    }
    return round(total_score, 2), detail


def classify_low_absorb_status(
    low_absorb_score: float,
    confirm_signal: str,
    not_break_low_volume_low: bool,
    price_above_ma5: bool,
    price_above_ma10: bool,
    reward_risk_ratio: float | None,
) -> str:
    if not not_break_low_volume_low:
        return "信号失效-跌破地量低点"
    if low_absorb_score >= 80 and confirm_signal != "无" and (reward_risk_ratio or 0) >= 2:
        return "可低吸-确认型"
    if low_absorb_score >= 75 and price_above_ma5 and price_above_ma10 and (reward_risk_ratio or 0) >= 1.5:
        return "可低吸-轻仓型"
    if low_absorb_score >= 70 and price_above_ma5:
        return "观察-等待放量确认"
    if low_absorb_score >= 60:
        return "观察-等待站回均线"
    return "弱观察-条件不足"


def build_volume_rank_maps(rows: list[KlineRow]) -> tuple[dict[dt.date, int], dict[dt.date, float]]:
    """Return volume rank and percentile maps for the given rows.

    Rank is ascending by volume: rank 1 means the lowest turnover hands in the
    sample. Percentile is also lower-is-better, so 5.00 means the day's volume
    is inside the lowest 5% of the sample.
    """
    sorted_rows = sorted(rows, key=lambda item: (item.volume, item.trade_date))
    total = len(sorted_rows)
    if total == 0:
        return {}, {}
    rank_by_date: dict[dt.date, int] = {}
    percentile_by_date: dict[dt.date, float] = {}
    for rank, row in enumerate(sorted_rows, start=1):
        rank_by_date[row.trade_date] = rank
        percentile_by_date[row.trade_date] = rank / total * 100
    return rank_by_date, percentile_by_date


def describe_confirm_signal(
    lookback_rows: list[KlineRow],
    hit_rows: list[KlineRow],
    pct_by_date: dict[dt.date, float | None],
) -> str:
    """Mark, but do not require, a post-exhaustion confirmation signal.

    Confirmation definition:
    - after the latest low-volume down day;
    - within the next 1-3 trading days;
    - pct_change > 0;
    - close breaks above the low-volume day high;
    - volume is greater than the average volume of the previous 5 trading days.
    """
    if not hit_rows:
        return "无"

    latest_hit = max(hit_rows, key=lambda item: item.trade_date)
    date_to_index = {row.trade_date: index for index, row in enumerate(lookback_rows)}
    hit_index = date_to_index.get(latest_hit.trade_date)
    if hit_index is None:
        return "无"

    previous_rows = lookback_rows[max(0, hit_index - 4) : hit_index + 1]
    previous_avg_volume = average([row.volume for row in previous_rows])
    if previous_avg_volume <= 0:
        return "无"

    for row in lookback_rows[hit_index + 1 : hit_index + 4]:
        pct_change = pct_by_date.get(row.trade_date)
        if (
            pct_change is not None
            and pct_change > 0
            and row.close_price > latest_hit.high_price
            and row.volume > previous_avg_volume
        ):
            return f"{row.trade_date:%Y-%m-%d} 放量阳线突破低量日高点"
    return "无"


def find_limit_price_dates(rows: list[KlineRow], threshold_pct: float) -> set[dt.date]:
    """Return dates that look like 10cm limit-up / limit-down days.

    The normal path uses the pct_change field from the K-line source.  If that
    field is missing, fall back to previous close inside the lookback window.
    The threshold defaults to 9.8 instead of exactly 10.0 to tolerate rounding
    and source differences.
    """
    result: set[dt.date] = set()
    previous_row: KlineRow | None = None
    for row in rows:
        pct_change = row.pct_change
        if pct_change is None and previous_row is not None and previous_row.close_price > 0:
            pct_change = (row.close_price - previous_row.close_price) / previous_row.close_price * 100
        if pct_change is not None and abs(pct_change) >= threshold_pct:
            result.add(row.trade_date)
        previous_row = row
    return result


def result_fieldnames() -> list[str]:
    return [
        "股票代码",
        "股票名称",
        "市场",
        "所属行业",
        "相关概念",
        "主题标签",
        "主题评分",
        "总市值(亿元)",
        "最近窗口交易日数",
        "命中天数",
        "命中日期",
        "命中下跌低量",
        "命中下跌低量排名",
        "命中下跌低量分位(%)",
        "最新成交量(手)",
        "最新成交量全样本排名",
        "最新成交量全样本分位(%)",
        "最新是否下跌日",
        "最新下跌量排名",
        "最新下跌量分位(%)",
        "最新量/下跌低量阈值",
        "下跌低量阈值",
        "下跌样本数",
        "排除涨跌停天数",
        "近20日均成交额(亿元)",
        "近5日/60日均量比",
        "近20日高点回撤(%)",
        "距60日低点(%)",
        "确认信号",
        "低吸综合评分",
        "最终评分",
        "建议状态",
        "卖盘枯竭评分",
        "整体缩量评分",
        "位置安全评分",
        "趋势质量评分",
        "确认信号评分",
        "风险收益评分",
        "最近低量日低点",
        "是否跌破低量日低点",
        "MA5",
        "MA10",
        "MA20",
        "MA60",
        "是否站上MA5",
        "是否站上MA10",
        "是否站上MA20",
        "参考止损价",
        "参考目标价",
        "盈亏比",
        "排序开始日",
        "排序结束日",
        "最新收盘价",
        "最新涨跌幅(%)",
    ]


def result_to_dict(row: ScreenResult) -> dict[str, str | int]:
    return {
        "股票代码": row.code,
        "股票名称": row.name,
        "市场": row.market,
        "所属行业": row.industry,
        "相关概念": row.concepts,
        "主题标签": " / ".join(row.theme_tags),
        "主题评分": format_number(row.theme_score, 2),
        "总市值(亿元)": format_number(row.total_market_cap_yi, 2),
        "最近窗口交易日数": row.recent_window_days,
        "命中天数": row.hit_days,
        "命中日期": " / ".join(row.hit_dates),
        "命中下跌低量": " / ".join(str(value) for value in row.hit_green_volumes),
        "命中下跌低量排名": " / ".join(str(value) for value in row.hit_green_ranks),
        "命中下跌低量分位(%)": " / ".join(
            format_number(value, 2) for value in row.hit_green_percentiles
        ),
        "最新成交量(手)": row.latest_volume if row.latest_volume is not None else "",
        "最新成交量全样本排名": row.latest_volume_rank if row.latest_volume_rank is not None else "",
        "最新成交量全样本分位(%)": format_number(row.latest_volume_percentile, 2),
        "最新是否下跌日": "是" if row.latest_is_down_day else "否",
        "最新下跌量排名": row.latest_down_volume_rank if row.latest_down_volume_rank is not None else "",
        "最新下跌量分位(%)": format_number(row.latest_down_volume_percentile, 2),
        "最新量/下跌低量阈值": format_number(row.latest_volume_to_cutoff_ratio, 3),
        "下跌低量阈值": row.green_cutoff_volume,
        "下跌样本数": row.green_sample_count,
        "排除涨跌停天数": row.excluded_limit_days,
        "近20日均成交额(亿元)": format_number(row.avg_amount_20d_yi, 2),
        "近5日/60日均量比": format_number(row.recent_volume_ratio, 3),
        "近20日高点回撤(%)": format_number(row.drop_20d_pct, 2),
        "距60日低点(%)": format_number(row.distance_from_60d_low_pct, 2),
        "确认信号": row.confirm_signal,
        "低吸综合评分": format_number(row.low_absorb_score, 2),
        "最终评分": format_number(row.final_score, 2),
        "建议状态": row.suggested_status,
        "卖盘枯竭评分": format_number(row.exhaustion_score, 2),
        "整体缩量评分": format_number(row.shrink_score, 2),
        "位置安全评分": format_number(row.position_score, 2),
        "趋势质量评分": format_number(row.trend_score, 2),
        "确认信号评分": format_number(row.confirm_score, 2),
        "风险收益评分": format_number(row.reward_risk_score, 2),
        "最近低量日低点": format_number(row.latest_hit_low_price, 3),
        "是否跌破低量日低点": "否" if row.not_break_low_volume_low else "是",
        "MA5": format_number(row.ma5, 3),
        "MA10": format_number(row.ma10, 3),
        "MA20": format_number(row.ma20, 3),
        "MA60": format_number(row.ma60, 3),
        "是否站上MA5": "是" if row.price_above_ma5 else "否",
        "是否站上MA10": "是" if row.price_above_ma10 else "否",
        "是否站上MA20": "是" if row.price_above_ma20 else "否",
        "参考止损价": format_number(row.stop_price, 3),
        "参考目标价": format_number(row.target_price, 3),
        "盈亏比": format_number(row.reward_risk_ratio, 2),
        "排序开始日": row.lookback_start,
        "排序结束日": row.lookback_end,
        "最新收盘价": format_number(row.latest_close, 3),
        "最新涨跌幅(%)": format_number(row.latest_pct_change, 3),
    }


def write_results(path: Path, rows: list[ScreenResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.DictWriter(file, fieldnames=result_fieldnames())
        writer.writeheader()
        for row in rows:
            writer.writerow(result_to_dict(row))


def write_markdown(path: Path, rows: list[ScreenResult]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    names = result_fieldnames()
    lines = [
        "| " + " | ".join(names) + " |",
        "| " + " | ".join(["---"] * len(names)) + " |",
    ]
    for row in rows:
        record = result_to_dict(row)
        lines.append("| " + " | ".join(markdown_cell(record[name]) for name in names) + " |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_run_state(args: argparse.Namespace, candidates_count: int, results_count: int) -> None:
    RUN_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUN_STATE_PATH.write_text(
        json.dumps(
            {
                "generated_at": f"{dt.datetime.now():%Y-%m-%d %H:%M:%S}",
                "effective_end_date": f"{args.effective_end_date:%Y-%m-%d}",
                "lookback_days": args.lookback_days,
                "lowest_down_percent": args.lowest_green_percent,
                "down_day_mode": args.down_day_mode,
                "recent_days": args.recent_days,
                "min_hit_days": args.min_hit_days,
                "exclude_limit_days": not args.keep_limit_days,
                "limit_pct_threshold": args.limit_pct_threshold,
                "min_avg_amount_yi": args.min_avg_amount_yi,
                "recent_volume_shrink_ratio": args.recent_volume_shrink_ratio,
                "max_20d_drop_pct": args.max_20d_drop_pct,
                "min_distance_from_60d_low_pct": args.min_distance_from_60d_low_pct,
                "max_latest_volume_percentile": args.max_latest_volume_percentile,
                "require_confirm_signal": args.require_confirm_signal,
                "require_not_break_low_volume_low": args.require_not_break_low_volume_low,
                "require_price_above_ma5": args.require_price_above_ma5,
                "require_price_above_ma10": args.require_price_above_ma10,
                "require_price_above_ma20": args.require_price_above_ma20,
                "min_reward_risk_ratio": args.min_reward_risk_ratio,
                "min_low_absorb_score": args.min_low_absorb_score,
                "min_final_score": args.min_final_score,
                "theme_csv": str(args.theme_csv_path),
                "theme_score_weight": args.theme_score_weight,
                "min_theme_score": args.min_theme_score,
                "require_theme": args.require_theme,
                "market_filter": args.market_filter,
                "market_index_code": args.market_index_code,
                "history_days": args.history_days,
                "min_market_cap_yi": args.min_market_cap_yi,
                "candidate_count": candidates_count,
                "result_count": results_count,
                "candidate_table": str(args.candidate_output_path),
                "result_csv": str(args.output_path),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def copy_latest_outputs(source_csv: Path, rows: list[ScreenResult]) -> None:
    write_results(LATEST_RESULT_PATH, rows)
    write_markdown(LATEST_RESULT_MD_PATH, rows)


def normalize_market_cap_yi(value: Any, column_name: str = "") -> float | None:
    raw_value = to_float(value)
    if raw_value is None:
        return None
    # 东方财富和 AkShare 的实时行情多数返回“元”，少数封装版本可能返回“亿元”。
    # 用列名和数量级双保险，避免把 2.3 万亿元误写成 2.3 万亿元“亿元”。
    if "亿" in column_name:
        return raw_value
    if abs(raw_value) >= 1_000_000:
        return raw_value / 100_000_000
    return raw_value


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str) and value.strip() in {"", "-", "None"}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_eastmoney_date(value: Any) -> str:
    text = str(value or "").strip()
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def format_number(value: float | int | None, digits: int = 2) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f}".rstrip("0").rstrip(".")


def markdown_cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", "<br>")


def main() -> int:
    args = parse_args()
    args.data_dir_path.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    candidates = load_or_refresh_candidates(args)
    candidates = [s for s in candidates if s.code not in EXCLUDED_STOCKS]
    if not candidates:
        print("候选表为空，请检查数据源或筛选条件。", file=sys.stderr)
        return 1

    args.theme_map = load_theme_map(args.theme_csv_path)
    if args.theme_map:
        print(f"已加载主题股票池 {args.theme_csv_path}，共 {len(args.theme_map)} 条。")
    else:
        print("未加载到主题股票池，仅使用股票名称/行业/概念关键词进行主题评分。")

    if args.force_renormalize:
        renormalize_all_klines(args.kline_dir_path)
    update_market_index_cache(args)
    market_ok, market_msg = market_filter_passed(args)
    print(f"大盘过滤：{market_msg}")
    if not market_ok:
        print("大盘环境未通过过滤，本次输出空结果。")
        results = []
    else:
        update_kline_cache(args, candidates)
        results = select_low_absorb_stocks(args, candidates)
    write_results(args.output_path, results)
    copy_latest_outputs(args.output_path, results)
    write_run_state(args, len(candidates), len(results))

    print(f"低吸结果已保存 {args.output_path}，共 {len(results)} 只。")
    print(f"最新结果副本 {LATEST_RESULT_PATH}")
    print("提示：默认下午 4 点后运行会包含今日；默认排除涨跌停日；默认按涨跌幅<0定义下跌日。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
