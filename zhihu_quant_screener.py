#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
zhihu_quant_screener.py

把“优质基本面 + 趋势确认 + 成交量配合 + 风险过滤”的文字策略量化为 A 股筛选器。

默认使用 AkShare，无需 Tushare Token；若提供 Tushare Token，可额外加入 ROE、营收同比、净利同比、资产负债率等基本面硬条件。

安装：
    pip install akshare pandas numpy tqdm openpyxl tushare

示例：
    python zhihu_quant_screener.py --min-market-cap-yi 100 --min-score 70 --workers 6

更严格：
    python zhihu_quant_screener.py \
      --min-market-cap-yi 300 \
      --min-avg-amount-yi 2 \
      --min-score 78 \
      --require-bull-ma \
      --require-macd-positive \
      --require-breakout-or-low-absorb \
      --workers 6

带 Tushare 基本面：
    python zhihu_quant_screener.py --tushare-token YOUR_TOKEN --min-roe 12 --min-netprofit-yoy 15 --min-revenue-yoy 10
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import json
import math
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

try:
    from tqdm import tqdm
except Exception:  # pragma: no cover
    tqdm = None


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
KLINE_DIR = DATA_DIR / "kline"
OUTPUT_DIR = PROJECT_DIR
RUN_STATE_PATH = DATA_DIR / "run_state.json"
CANDIDATE_TABLE_PATH = DATA_DIR / "candidate_table.csv"
DEFAULT_AFTER_HOUR = 16
MAIN_BOARD_PREFIXES = ("000", "001", "002", "003", "600", "601", "603", "605")
RISK_NAME_WORDS = ("ST", "*ST", "退", "退市")


@dataclass
class ScreenResult:
    code: str
    name: str
    close: float
    pct_chg: float
    total_mv_yi: float
    amount_yi: float
    avg_amount20_yi: float
    pe: float
    pb: float
    score: float
    grade: str
    pattern: str
    reasons: str

    # technical fields
    ma5: float
    ma10: float
    ma20: float
    ma60: float
    ma120: float
    dif: float
    dea: float
    macd_hist: float
    volume_ratio_5: float
    volume_ratio_20: float
    close_to_60d_high_pct: float
    close_from_120d_low_pct: float
    drawdown_120d_pct: float
    low_absorb_days: int
    breakout_flag: bool
    pullback_flag: bool
    bull_ma_flag: bool
    macd_positive_flag: bool

    # optional fundamentals
    roe: Optional[float] = None
    revenue_yoy: Optional[float] = None
    netprofit_yoy: Optional[float] = None
    debt_to_assets: Optional[float] = None


# ---------------------------
# basic helpers
# ---------------------------

def safe_float(x, default: float = np.nan) -> float:
    if x is None:
        return default
    if isinstance(x, (int, float, np.number)):
        if pd.isna(x):
            return default
        return float(x)
    s = str(x).strip().replace(",", "").replace("%", "")
    if s in ("", "-", "--", "nan", "None"):
        return default
    try:
        return float(s)
    except Exception:
        return default


def is_main_board(code: str) -> bool:
    code = str(code).zfill(6)
    return code.startswith(MAIN_BOARD_PREFIXES)


def is_risky_name(name: str) -> bool:
    upper = str(name).upper()
    return any(w in upper for w in RISK_NAME_WORDS)


def to_tushare_code(code: str) -> str:
    code = str(code).zfill(6)
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    return f"{code}.SZ"


def grade_from_score(score: float) -> str:
    if score >= 85:
        return "A"
    if score >= 75:
        return "B"
    if score >= 65:
        return "C"
    return "D"


def latest_trade_dates(days_back: int = 430) -> Tuple[str, str]:
    end = datetime.now()
    start = end - timedelta(days=days_back)
    return start.strftime("%Y%m%d"), end.strftime("%Y%m%d")


def resolve_effective_end_date(args: argparse.Namespace) -> datetime:
    if args.end_date:
        return datetime.strptime(args.end_date, "%Y%m%d")

    now = datetime.now()
    if args.include_today:
        return now
    if args.exclude_today:
        return now - timedelta(days=1)
    if now.hour >= DEFAULT_AFTER_HOUR:
        return now
    return now - timedelta(days=1)


def history_window(effective_end: datetime, days_back: int) -> Tuple[str, str]:
    start = effective_end - timedelta(days=days_back)
    return start.strftime("%Y%m%d"), effective_end.strftime("%Y%m%d")


# ---------------------------
# indicators
# ---------------------------

def calc_macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[pd.Series, pd.Series, pd.Series]:
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    dif = ema_fast - ema_slow
    dea = dif.ewm(span=signal, adjust=False).mean()
    hist = 2 * (dif - dea)
    return dif, dea, hist


def pct_rank(value: float, series: pd.Series) -> float:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) == 0 or pd.isna(value):
        return np.nan
    return float((s <= value).mean() * 100)


def enrich_hist(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize AkShare daily kline dataframe and add indicators."""
    if df is None or df.empty:
        return pd.DataFrame()

    col_map = {
        "日期": "date",
        "开盘": "open",
        "收盘": "close",
        "最高": "high",
        "最低": "low",
        "成交量": "volume",
        "成交额": "amount",
        "涨跌幅": "pct_chg",
        "换手率": "turnover",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}).copy()
    required = ["date", "open", "close", "high", "low", "volume", "amount"]
    for c in required:
        if c not in df.columns:
            return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    for c in ["open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values("date").dropna(subset=["close", "volume"]).reset_index(drop=True)
    if "pct_chg" not in df.columns or df["pct_chg"].isna().all():
        df["pct_chg"] = df["close"].pct_change() * 100

    for n in (5, 10, 20, 60, 120):
        df[f"ma{n}"] = df["close"].rolling(n).mean()
        df[f"vol_ma{n}"] = df["volume"].rolling(n).mean()
        df[f"amount_ma{n}"] = df["amount"].rolling(n).mean()

    df["dif"], df["dea"], df["macd_hist"] = calc_macd(df["close"])
    df["high60"] = df["high"].rolling(60).max()
    df["high120"] = df["high"].rolling(120).max()
    df["low120"] = df["low"].rolling(120).min()
    df["ret_20d"] = df["close"].pct_change(20) * 100
    df["ret_60d"] = df["close"].pct_change(60) * 100
    return df


# ---------------------------
# data fetchers
# ---------------------------

def normalize_spot_df(df: pd.DataFrame) -> pd.DataFrame:
    """把不同数据源返回的实时行情统一成脚本内部字段。"""
    if df is None or df.empty:
        raise RuntimeError("实时行情接口未返回数据。")

    rename = {
        "代码": "code",
        "名称": "name",
        "最新价": "close",
        "涨跌幅": "pct_chg",
        "成交额": "amount",
        "总市值": "total_mv",
        "流通市值": "float_mv",
        "市盈率-动态": "pe",
        "市净率": "pb",
        "换手率": "turnover",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns}).copy()
    if "code" not in df.columns or "name" not in df.columns:
        raise RuntimeError(f"实时行情字段不完整，当前字段：{list(df.columns)[:20]}")

    df["code"] = df["code"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
    df = df[df["code"].str.len() == 6].copy()
    for c in ["close", "pct_chg", "amount", "total_mv", "float_mv", "pe", "pb", "turnover"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        else:
            df[c] = np.nan
    return df


def candidate_table_is_fresh(path: Path, max_age_hours: int = 20) -> bool:
    if not path.exists():
        return False
    modified_at = datetime.fromtimestamp(path.stat().st_mtime)
    return datetime.now() - modified_at <= timedelta(hours=max_age_hours)


def read_candidate_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    return normalize_spot_df(df)


def write_candidate_table(path: Path, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    keep_cols = ["code", "name", "close", "pct_chg", "amount", "total_mv", "float_mv", "pe", "pb", "turnover"]
    out = out[[col for col in keep_cols if col in out.columns]].copy()
    out.to_csv(path, index=False, encoding="utf-8-sig")


def load_or_refresh_candidates(args: argparse.Namespace) -> pd.DataFrame:
    if not args.refresh_candidates and candidate_table_is_fresh(args.candidate_output_path):
        cached = read_candidate_table(args.candidate_output_path)
        if cached is not None and not cached.empty:
            print(f"复用候选表 {args.candidate_output_path}，共 {len(cached)} 只。")
            return cached

    print("正在刷新候选表...")
    try:
        fresh = fetch_akshare_spot(retry=args.spot_retry, sleep=args.spot_retry_sleep)
        write_candidate_table(args.candidate_output_path, fresh)
        print(f"候选表已保存 {args.candidate_output_path}，共 {len(fresh)} 只。")
        return fresh
    except Exception as exc:
        cached = read_candidate_table(args.candidate_output_path)
        if cached is not None and not cached.empty:
            print(
                f"[WARN] 候选表刷新失败，改用本地旧缓存 {args.candidate_output_path}，共 {len(cached)} 只。失败原因: {exc}",
                file=sys.stderr,
            )
            return cached
        raise


def fetch_eastmoney_spot_direct(retry: int = 5, sleep: float = 1.5) -> pd.DataFrame:
    """
    AkShare 的 stock_zh_a_spot_em 底层也是东方财富接口。
    部分 Windows/网络环境会被远端主动断开；这里用 requests + headers + 多 host 兜底。
    """
    import requests

    fields = "f12,f14,f2,f3,f6,f20,f21,f9,f23,f8"
    hosts = [
        "https://82.push2.eastmoney.com",
        "https://push2.eastmoney.com",
        "https://33.push2.eastmoney.com",
        "https://63.push2.eastmoney.com",
    ]
    # m:0 深市；m:1 沪市。t:6/t:80/t:2/t:23 可覆盖常见 A 股板块。
    fs = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
        "Referer": "https://quote.eastmoney.com/center/gridlist.html",
        "Accept": "application/json,text/plain,*/*",
        "Connection": "close",
    }

    last_exc = None
    for i in range(retry):
        host = hosts[i % len(hosts)]
        try:
            rows = []
            pz = 500
            total = None
            session = requests.Session()
            for page in range(1, 40):
                params = {
                    "pn": page,
                    "pz": pz,
                    "po": 1,
                    "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2,
                    "invt": 2,
                    "fid": "f3",
                    "fs": fs,
                    "fields": fields,
                }
                r = session.get(f"{host}/api/qt/clist/get", params=params, headers=headers, timeout=15)
                r.raise_for_status()
                payload = r.json()
                data = payload.get("data") or {}
                diff = data.get("diff") or []
                if isinstance(diff, dict):
                    diff = list(diff.values())
                if total is None:
                    total = int(data.get("total") or 0)
                if not diff:
                    break
                rows.extend(diff)
                if total and len(rows) >= total:
                    break
                time.sleep(0.05)

            if not rows:
                raise RuntimeError("东方财富 direct 接口返回空数据")

            out = pd.DataFrame(rows).rename(columns={
                "f12": "code",
                "f14": "name",
                "f2": "close",
                "f3": "pct_chg",
                "f6": "amount",
                "f20": "total_mv",
                "f21": "float_mv",
                "f9": "pe",
                "f23": "pb",
                "f8": "turnover",
            })
            return normalize_spot_df(out)
        except Exception as e:
            last_exc = e
            print(f"[WARN] 东方财富实时行情 direct 第 {i + 1}/{retry} 次失败：{e}")
            time.sleep(sleep * (i + 1))

    raise RuntimeError("东方财富实时行情 direct 兜底也失败。") from last_exc


def fetch_tencent_spot_direct(retry: int = 3, sleep: float = 1.0) -> pd.DataFrame:
    import requests

    prefixes = ("000", "001", "002", "003", "600", "601", "603", "605")
    codes = [f"{prefix}{suffix:03d}" for prefix in prefixes for suffix in range(1000)]
    batch_size = 80

    session = requests.Session()
    session.headers.update(
        {
            "Referer": "https://gu.qq.com/",
            "Origin": "https://gu.qq.com",
            "Accept": "*/*",
            "Connection": "close",
        }
    )

    rows: Dict[str, Dict[str, float | str]] = {}
    for start in range(0, len(codes), batch_size):
        batch = codes[start : start + batch_size]
        symbols = ",".join((("sh" if code.startswith("6") else "sz") + code) for code in batch)
        url = "https://qt.gtimg.cn/q=" + symbols
        text = ""
        last_exc = None
        for i in range(retry):
            resp = None
            try:
                resp = session.get(url, timeout=15)
                resp.raise_for_status()
                resp.encoding = "gbk"
                text = resp.text
                break
            except Exception as exc:
                last_exc = exc
                if i < retry - 1:
                    time.sleep(sleep * (i + 1))
            finally:
                if resp is not None:
                    resp.close()
        if not text:
            raise RuntimeError(f"腾讯行情请求失败: {last_exc}")

        for quote_text in re.findall(r'v_[^=]+="(.*?)";', text, flags=re.S):
            parsed = parse_tencent_spot_quote(quote_text)
            if parsed is not None:
                rows[parsed["code"]] = parsed
        time.sleep(0.04)

    if not rows:
        raise RuntimeError("腾讯行情返回空数据")
    return normalize_spot_df(pd.DataFrame(rows.values()))


def parse_tencent_spot_quote(quote_text: str) -> Optional[Dict[str, float | str]]:
    if not quote_text:
        return None
    parts = quote_text.split("~")
    if len(parts) < 46:
        return None

    code = re.sub(r"\D", "", str(parts[2] if len(parts) > 2 else "")).zfill(6)
    name = str(parts[1] if len(parts) > 1 else "").strip()
    if len(code) != 6 or not name:
        return None

    close = safe_float(parts[3] if len(parts) > 3 else np.nan)
    pct_chg = safe_float(parts[32] if len(parts) > 32 else np.nan)
    total_mv_yi = np.nan
    for idx in (45, 46, 44):
        if len(parts) <= idx:
            continue
        value = safe_float(parts[idx], default=np.nan)
        if not math.isnan(value) and value > 0:
            total_mv_yi = value
            break

    total_mv = total_mv_yi * 1e8 if not math.isnan(total_mv_yi) else np.nan
    return {
        "code": code,
        "name": name,
        "close": close,
        "pct_chg": pct_chg,
        "amount": np.nan,
        "total_mv": total_mv,
        "float_mv": np.nan,
        "pe": np.nan,
        "pb": np.nan,
        "turnover": np.nan,
    }


def fetch_akshare_spot(retry: int = 5, sleep: float = 1.5) -> pd.DataFrame:
    """获取 A 股实时行情：AkShare -> 腾讯 -> 东方财富。"""
    try:
        import akshare as ak
    except Exception as e:
        raise RuntimeError("缺少 akshare。请先执行：pip install akshare") from e

    methods = []
    if hasattr(ak, "stock_zh_a_spot_em"):
        methods.append(("ak.stock_zh_a_spot_em", ak.stock_zh_a_spot_em))
    # 交易所拆分接口有时比全市场接口稳定；存在时也试一次。
    if hasattr(ak, "stock_sh_a_spot_em") and hasattr(ak, "stock_sz_a_spot_em"):
        methods.append((
            "ak.stock_sh_a_spot_em + ak.stock_sz_a_spot_em",
            lambda: pd.concat([ak.stock_sh_a_spot_em(), ak.stock_sz_a_spot_em()], ignore_index=True),
        ))

    last_exc = None
    for name, func in methods:
        for i in range(retry):
            try:
                df = func()
                df = normalize_spot_df(df)
                if not df.empty:
                    return df
            except Exception as e:
                last_exc = e
                print(f"[WARN] {name} 第 {i + 1}/{retry} 次失败：{e}")
                time.sleep(sleep * (i + 1))

    # Some environments can access Sina's quote endpoint but not Eastmoney.
    if hasattr(ak, "stock_zh_a_spot"):
        print("[WARN] AkShare 实时行情失败，尝试 ak.stock_zh_a_spot 兜底接口。")
        for i in range(retry):
            try:
                df = ak.stock_zh_a_spot()
                df = normalize_spot_df(df)
                if not df.empty:
                    print("[INFO] 已切换到 ak.stock_zh_a_spot 数据源。")
                    return df
            except Exception as e:
                last_exc = e
                print(f"[WARN] ak.stock_zh_a_spot 第 {i + 1}/{retry} 次失败：{e}")
                time.sleep(sleep * (i + 1))

    print("[WARN] AkShare 实时行情失败，切换到腾讯行情兜底接口。")
    try:
        return fetch_tencent_spot_direct(retry=max(2, retry // 2 + 1), sleep=sleep)
    except Exception as e:
        last_exc = last_exc or e

    print("[WARN] 腾讯行情失败，切换到东方财富 direct 兜底接口。")
    try:
        return fetch_eastmoney_spot_direct(retry=retry, sleep=sleep)
    except Exception as e:
        last_exc = last_exc or e

    # Last-resort fallback: only use stock universe (code + name), keep run alive.
    if hasattr(ak, "stock_info_a_code_name"):
        print("[WARN] 实时行情接口全部失败，降级为代码列表模式（无实时市值/估值/成交额）。")
        try:
            base = ak.stock_info_a_code_name()
            if base is not None and not base.empty:
                out = pd.DataFrame({
                    "code": base.get("code"),
                    "name": base.get("name"),
                    "close": np.nan,
                    "pct_chg": np.nan,
                    "amount": np.nan,
                    "total_mv": np.nan,
                    "float_mv": np.nan,
                    "pe": np.nan,
                    "pb": np.nan,
                    "turnover": np.nan,
                })
                out["code"] = out["code"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)
                out = out[out["code"].str.len() == 6].copy()
                if not out.empty:
                    print(f"[INFO] 已降级为代码列表模式，共 {len(out)} 只。")
                    return out
        except Exception as fallback_e:
            last_exc = fallback_e

    raise RuntimeError(
        "获取A股实时行情失败。通常是东方财富/网络连接被远端断开，不是筛选逻辑错误。"
        "请稍后重试，或降低并发，或切换网络/VPN/代理后重试。"
    ) from last_exc


def fetch_hist_remote(code: str, start_date: str, end_date: str, adjust: str = "qfq", retry: int = 4) -> pd.DataFrame:
    code = str(code).zfill(6)
    errors: List[str] = []

    # 1) Eastmoney direct first: usually fastest and has amount/turnover fields.
    try:
        df = fetch_eastmoney_hist_direct(code=code, start_date=start_date, end_date=end_date, adjust=adjust, retry=retry)
        if df is not None and not df.empty:
            return enrich_hist(df)
        errors.append("Eastmoney direct returned empty data")
    except Exception as exc:
        errors.append(f"Eastmoney direct failed: {exc}")

    # 2) AkShare fallback.
    try:
        import akshare as ak
        last_exc = None
        for i in range(retry + 1):
            try:
                raw = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust=adjust,
                )
                df = enrich_hist(raw)
                if df is not None and not df.empty:
                    return df
                errors.append("AkShare returned empty data")
                break
            except Exception as exc:
                last_exc = exc
                time.sleep(0.6 * (i + 1))
        if last_exc is not None:
            errors.append(f"AkShare failed: {last_exc}")
    except Exception as exc:
        errors.append(f"AkShare import failed: {exc}")

    # 3) Sina fallback: no native amount, estimate with volume*close to keep amount-based filters usable.
    try:
        df = fetch_sina_hist_direct(code=code, start_date=start_date, end_date=end_date, retry=retry)
        if df is not None and not df.empty:
            return enrich_hist(df)
        errors.append("Sina returned empty data")
    except Exception as exc:
        errors.append(f"Sina failed: {exc}")

    raise RuntimeError(" | ".join(errors))


def kline_cache_path(kline_dir: Path, code: str) -> Path:
    return kline_dir / f"{str(code).zfill(6)}.csv"


def read_hist_cache(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()
    if df is None or df.empty:
        return pd.DataFrame()
    return enrich_hist(df)


def write_hist_cache(path: Path, df: pd.DataFrame) -> None:
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    out = df.copy()
    keep_cols = ["date", "open", "close", "high", "low", "volume", "amount", "pct_chg", "turnover"]
    out = out[[col for col in keep_cols if col in out.columns]].copy()
    out["date"] = pd.to_datetime(out["date"]).dt.strftime("%Y-%m-%d")
    out = out.sort_values("date").drop_duplicates(subset=["date"], keep="last")
    out.to_csv(path, index=False, encoding="utf-8-sig")


def merge_hist_cache(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    if existing is None or existing.empty:
        return fetched.copy()
    if fetched is None or fetched.empty:
        return existing.copy()
    merged = pd.concat([existing, fetched], ignore_index=True)
    merged["date"] = pd.to_datetime(merged["date"])
    merged = merged.sort_values("date").drop_duplicates(subset=["date"], keep="last").reset_index(drop=True)
    return merged


def trim_hist_cache(df: pd.DataFrame, earliest_date: datetime) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    out["date"] = pd.to_datetime(out["date"])
    out = out[out["date"] >= pd.Timestamp(earliest_date.date())].reset_index(drop=True)
    return out


def update_one_kline_cache(
    code: str,
    kline_dir: Path,
    history_begin: datetime,
    effective_end: datetime,
    adjust: str,
    force: bool,
) -> Tuple[str, int, str]:
    path = kline_cache_path(kline_dir, code)
    existing = pd.DataFrame() if force else read_hist_cache(path)

    if not existing.empty:
        last_date = pd.to_datetime(existing["date"]).max().to_pydatetime()
        fetch_begin = max(history_begin, last_date - timedelta(days=10))
    else:
        fetch_begin = history_begin

    if not force and not existing.empty and pd.to_datetime(existing["date"]).max().date() >= effective_end.date():
        trimmed = trim_hist_cache(existing, history_begin)
        if len(trimmed) != len(existing):
            write_hist_cache(path, trimmed)
        return str(code).zfill(6), len(trimmed), "cached"

    fetched = fetch_hist_remote(
        code=str(code).zfill(6),
        start_date=fetch_begin.strftime("%Y%m%d"),
        end_date=effective_end.strftime("%Y%m%d"),
        adjust=adjust,
    )
    merged = merge_hist_cache(existing, fetched)
    trimmed = trim_hist_cache(merged, history_begin)
    write_hist_cache(path, trimmed)
    return str(code).zfill(6), len(trimmed), "updated"


def update_kline_cache(args: argparse.Namespace, rows: List[Dict]) -> None:
    if args.skip_kline_update:
        print("已跳过联网更新日线，只使用本地缓存。")
        return

    args.kline_dir_path.mkdir(parents=True, exist_ok=True)
    history_begin = args.effective_end_date - timedelta(days=args.history_days + 14)
    total = len(rows)
    updated = 0
    cached = 0
    failed = 0

    print(f"正在增量更新日线：{total} 只，目标截止 {args.effective_end_date:%Y-%m-%d}...")
    with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = {
            ex.submit(
                update_one_kline_cache,
                str(row.get("code", "")).zfill(6),
                args.kline_dir_path,
                history_begin,
                args.effective_end_date,
                args.adjust,
                args.force_kline_update,
            ): row
            for row in rows
        }
        for index, fut in enumerate(cf.as_completed(futs), start=1):
            row = futs[fut]
            try:
                _code, _count, status = fut.result()
                if status == "updated":
                    updated += 1
                else:
                    cached += 1
            except Exception as exc:
                failed += 1
                print(f"[WARN] {row.get('code')} {row.get('name')} 日线更新失败: {exc}", file=sys.stderr)
            if index % 50 == 0 or index == total:
                print(f"日线进度 {index}/{total}，更新 {updated}，复用 {cached}，失败 {failed}。")


def write_run_state(
    args: argparse.Namespace,
    candidate_count: int,
    result_count: int,
    output_path: Path,
    archive_csv_path: Path,
) -> None:
    RUN_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    state = {
        "generated_at": f"{datetime.now():%Y-%m-%d %H:%M:%S}",
        "effective_end_date": f"{args.effective_end_date:%Y-%m-%d}",
        "history_days": args.history_days,
        "adjust": args.adjust,
        "candidate_count": candidate_count,
        "result_count": result_count,
        "candidate_table": str(args.candidate_output_path),
        "kline_dir": str(args.kline_dir_path),
        "result_csv": str(output_path.resolve()),
        "result_archive_csv": str(archive_csv_path.resolve()),
        "min_score": args.min_score,
        "min_market_cap_yi": args.min_market_cap_yi,
        "min_avg_amount_yi": args.min_avg_amount_yi,
        "main_board_only": args.main_board_only,
        "skip_kline_update": args.skip_kline_update,
        "force_kline_update": args.force_kline_update,
        "update_workers": args.workers,
        "screen_workers": args.screen_workers,
    }
    RUN_STATE_PATH.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_outputs_with_archive(
    out_df: pd.DataFrame,
    args: argparse.Namespace,
) -> Tuple[Path, Optional[Path], Path, Optional[Path]]:
    output = Path(args.output)
    out_df.to_csv(output, index=False, encoding="utf-8-sig")

    output_xlsx: Optional[Path] = None
    try:
        output_xlsx = output.with_suffix(".xlsx")
        out_df.to_excel(output_xlsx, index=False)
    except Exception:
        output_xlsx = None

    archive_dir = args.data_dir_path / "result_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_csv = archive_dir / f"{output.stem}_{args.effective_end_date:%Y%m%d}_{stamp}.csv"
    out_df.to_csv(archive_csv, index=False, encoding="utf-8-sig")

    archive_xlsx: Optional[Path] = None
    try:
        archive_xlsx = archive_csv.with_suffix(".xlsx")
        out_df.to_excel(archive_xlsx, index=False)
    except Exception:
        archive_xlsx = None

    return output, output_xlsx, archive_csv, archive_xlsx


def load_hist_from_cache(
    code: str,
    args: argparse.Namespace,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    path = kline_cache_path(args.kline_dir_path, code)
    cached = read_hist_cache(path)
    if cached is not None and not cached.empty:
        cached = cached[
            (cached["date"] >= pd.to_datetime(start_date))
            & (cached["date"] <= pd.to_datetime(end_date))
        ].reset_index(drop=True)
        if not cached.empty:
            return cached
    return fetch_hist_remote(code, start_date, end_date, adjust=args.adjust)


def _secid_for_code(code: str) -> str:
    return ("1." if str(code).startswith("6") else "0.") + str(code).zfill(6)


def fetch_eastmoney_hist_direct(
    code: str,
    start_date: str,
    end_date: str,
    adjust: str = "qfq",
    retry: int = 4,
    sleep: float = 0.8,
) -> pd.DataFrame:
    import requests

    hosts = [
        "https://push2his.eastmoney.com",
        "https://33.push2his.eastmoney.com",
        "https://63.push2his.eastmoney.com",
        "https://82.push2his.eastmoney.com",
    ]
    fqt_map = {"": "0", "qfq": "1", "hfq": "2"}
    fqt = fqt_map.get((adjust or "").lower(), "1")
    last_exc = None

    for i in range(retry + 1):
        host = hosts[i % len(hosts)]
        try:
            params = {
                "secid": _secid_for_code(code),
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",
                "fqt": fqt,
                "beg": start_date,
                "end": end_date,
                "lmt": "2000",
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            }
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
                ),
                "Referer": "https://quote.eastmoney.com/",
                "Accept": "application/json,text/plain,*/*",
                "Connection": "close",
            }
            response = requests.get(f"{host}/api/qt/stock/kline/get", params=params, headers=headers, timeout=15)
            response.raise_for_status()
            payload = response.json()
            items = ((payload.get("data") or {}).get("klines")) or []
            if not items:
                return pd.DataFrame()

            rows = []
            for item in items:
                parts = str(item).split(",")
                if len(parts) < 11:
                    continue
                try:
                    close_price = safe_float(parts[2])
                    volume = safe_float(parts[5])
                    amount = safe_float(parts[6])
                    # Eastmoney volume is usually in lots(手); convert to shares for consistency.
                    if not math.isnan(volume):
                        volume = volume * 100.0
                    rows.append(
                        {
                            "date": parts[0],
                            "open": safe_float(parts[1]),
                            "close": close_price,
                            "high": safe_float(parts[3]),
                            "low": safe_float(parts[4]),
                            "volume": volume,
                            "amount": amount,
                            "pct_chg": safe_float(parts[8]),
                            "turnover": safe_float(parts[10]),
                        }
                    )
                except Exception:
                    continue
            return pd.DataFrame(rows)
        except Exception as exc:
            last_exc = exc
            time.sleep(sleep * (i + 1))
    raise RuntimeError(f"eastmoney kline failed: {last_exc}") from last_exc


def fetch_sina_hist_direct(
    code: str,
    start_date: str,
    end_date: str,
    retry: int = 4,
    sleep: float = 0.8,
) -> pd.DataFrame:
    import requests

    symbol = ("sh" if str(code).startswith("6") else "sz") + str(code).zfill(6)
    start_dt = datetime.strptime(start_date, "%Y%m%d")
    end_dt = datetime.strptime(end_date, "%Y%m%d")
    url = "https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData"
    last_exc = None

    for i in range(retry + 1):
        try:
            resp = requests.get(
                url,
                params={"symbol": symbol, "scale": "240", "ma": "no", "datalen": "2000"},
                timeout=15,
            )
            resp.raise_for_status()
            text = resp.text.strip()
            data = []
            try:
                data = json.loads(text)
            except Exception:
                match = re.search(r"\((\[.*\])\)\s*;?\s*$", text, flags=re.S)
                if not match:
                    match = re.search(r"=\s*(\[.*\])\s*;?\s*$", text, flags=re.S)
                if match:
                    data = json.loads(match.group(1))
            if not isinstance(data, list):
                return pd.DataFrame()

            rows = []
            prev_close = None
            for item in data:
                day = str(item.get("day") or "")[:10]
                try:
                    dt_day = datetime.strptime(day, "%Y-%m-%d")
                except Exception:
                    continue
                if dt_day < start_dt or dt_day > end_dt:
                    continue
                close_price = safe_float(item.get("close"))
                volume = safe_float(item.get("volume"))
                amount = close_price * volume if not math.isnan(close_price) and not math.isnan(volume) else np.nan
                pct_chg = np.nan
                if prev_close and prev_close > 0 and not math.isnan(close_price):
                    pct_chg = (close_price / prev_close - 1.0) * 100.0
                rows.append(
                    {
                        "date": day,
                        "open": safe_float(item.get("open")),
                        "close": close_price,
                        "high": safe_float(item.get("high")),
                        "low": safe_float(item.get("low")),
                        "volume": volume,
                        "amount": amount,
                        "pct_chg": pct_chg,
                        "turnover": np.nan,
                    }
                )
                if not math.isnan(close_price):
                    prev_close = close_price
            return pd.DataFrame(rows)
        except Exception as exc:
            last_exc = exc
            time.sleep(sleep * (i + 1))
    raise RuntimeError(f"sina kline failed: {last_exc}") from last_exc


class TushareFundamentalFetcher:
    def __init__(self, token: Optional[str], start_date: str, end_date: str):
        self.enabled = bool(token)
        self.start_date = start_date
        self.end_date = end_date
        self.pro = None
        if self.enabled:
            import tushare as ts
            self.pro = ts.pro_api(token)

    def fetch(self, code: str) -> Dict[str, Optional[float]]:
        if not self.enabled:
            return {"roe": None, "revenue_yoy": None, "netprofit_yoy": None, "debt_to_assets": None}
        try:
            ts_code = to_tushare_code(code)
            df = self.pro.fina_indicator(
                ts_code=ts_code,
                start_date=self.start_date,
                end_date=self.end_date,
                fields="ts_code,end_date,roe,or_yoy,netprofit_yoy,debt_to_assets",
            )
            if df is None or df.empty:
                return {"roe": None, "revenue_yoy": None, "netprofit_yoy": None, "debt_to_assets": None}
            df = df.sort_values("end_date")
            latest = df.iloc[-1]
            return {
                "roe": safe_float(latest.get("roe")),
                "revenue_yoy": safe_float(latest.get("or_yoy")),
                "netprofit_yoy": safe_float(latest.get("netprofit_yoy")),
                "debt_to_assets": safe_float(latest.get("debt_to_assets")),
            }
        except Exception:
            return {"roe": None, "revenue_yoy": None, "netprofit_yoy": None, "debt_to_assets": None}


# ---------------------------
# strategy logic
# ---------------------------

def detect_low_absorb(
    hist: pd.DataFrame,
    lookback_days: int,
    recent_days: int,
    low_volume_percentile: float,
    min_hit_days: int,
) -> Tuple[bool, int, str]:
    """
    低吸痕迹：最近 recent_days 内，至少 min_hit_days 个“非跌停阴线/下跌日”的成交量，
    处于过去 lookback_days 成交量的低位分位。

    这对应文章/图中常见的“缩量回调、洗盘、跌不动”形态。
    """
    if len(hist) < max(lookback_days, 60):
        return False, 0, "数据不足"

    recent = hist.tail(recent_days).copy()
    base = hist.tail(lookback_days).copy()
    hit = 0
    details = []
    for _, row in recent.iterrows():
        pct = safe_float(row.get("pct_chg"))
        vol = safe_float(row.get("volume"))
        # 排除涨跌停导致的极端低成交量，避免误把“一字跌停/一字涨停”当成缩量洗盘
        non_limit = abs(pct) < 9.5
        down_or_green = pct <= 0.5  # 含小阳/十字星，避免过窄
        rank = pct_rank(vol, base["volume"])
        if non_limit and down_or_green and rank <= low_volume_percentile:
            hit += 1
            details.append(f"{row['date'].strftime('%m-%d')}量分位{rank:.1f}%")
    ok = hit >= min_hit_days
    return ok, hit, ";".join(details)


def score_one_stock(
    row: pd.Series,
    hist: pd.DataFrame,
    fina: Dict[str, Optional[float]],
    args: argparse.Namespace,
) -> Optional[ScreenResult]:
    code = str(row.get("code", "")).zfill(6)
    name = str(row.get("name", ""))

    if hist is None or hist.empty or len(hist) < 140:
        return None

    last = hist.iloc[-1]
    prev = hist.iloc[-2]

    close = safe_float(last.get("close"))
    pct_chg = safe_float(last.get("pct_chg"))
    amount_yi = safe_float(row.get("amount"), safe_float(last.get("amount"))) / 1e8
    total_mv_yi = safe_float(row.get("total_mv")) / 1e8
    pe = safe_float(row.get("pe"))
    pb = safe_float(row.get("pb"))

    avg_amount20_yi = safe_float(last.get("amount_ma20")) / 1e8
    ma5 = safe_float(last.get("ma5"))
    ma10 = safe_float(last.get("ma10"))
    ma20 = safe_float(last.get("ma20"))
    ma60 = safe_float(last.get("ma60"))
    ma120 = safe_float(last.get("ma120"))
    dif = safe_float(last.get("dif"))
    dea = safe_float(last.get("dea"))
    macd_hist = safe_float(last.get("macd_hist"))

    if any(math.isnan(x) for x in [close, ma20, ma60, ma120, avg_amount20_yi]):
        return None

    # 硬过滤：市场、名称、市值、流动性、估值区间
    if not is_main_board(code) and args.main_board_only:
        return None
    if is_risky_name(name):
        return None
    if total_mv_yi < args.min_market_cap_yi:
        return None
    if avg_amount20_yi < args.min_avg_amount_yi:
        return None
    if not (args.min_price <= close <= args.max_price):
        return None
    if pe <= 0 or pe > args.max_pe:
        return None
    if pb <= 0 or pb > args.max_pb:
        return None

    # 位置/风险
    high60_prev = safe_float(hist["high"].tail(61).head(60).max())
    high120 = safe_float(last.get("high120"))
    low120 = safe_float(last.get("low120"))
    close_to_60d_high_pct = (close / high60_prev - 1) * 100 if high60_prev > 0 else np.nan
    close_from_120d_low_pct = (close / low120 - 1) * 100 if low120 > 0 else np.nan
    drawdown_120d_pct = (close / high120 - 1) * 100 if high120 > 0 else np.nan

    # 技术条件
    bull_ma_flag = bool(close > ma20 and ma20 > ma60 and ma60 >= ma120 * (1 - args.ma120_tolerance_pct / 100))
    strong_bull_flag = bool(ma5 > ma10 > ma20 > ma60 and close > ma5)
    macd_positive_flag = bool(dif > dea and dif > 0)
    macd_improving_flag = bool(hist["macd_hist"].tail(3).is_monotonic_increasing)
    macd_golden_recent = bool(((hist["dif"].shift(1) <= hist["dea"].shift(1)) & (hist["dif"] > hist["dea"])).tail(5).any())

    volume_ratio_5 = safe_float(last.get("volume")) / safe_float(last.get("vol_ma5")) if safe_float(last.get("vol_ma5")) > 0 else np.nan
    volume_ratio_20 = safe_float(last.get("volume")) / safe_float(last.get("vol_ma20")) if safe_float(last.get("vol_ma20")) > 0 else np.nan

    breakout_flag = bool(
        close >= high60_prev * (1 - args.breakout_near_pct / 100)
        and volume_ratio_5 >= args.breakout_min_volume_ratio
        and pct_chg >= args.breakout_min_pct_chg
        and close > ma20
    )

    # 回踩低吸：中期趋势未坏，离 60/120 均线不远，近 N 天有缩量下跌/阴线
    low_absorb_ok, low_absorb_days, low_absorb_detail = detect_low_absorb(
        hist=hist,
        lookback_days=args.low_absorb_lookback_days,
        recent_days=args.low_absorb_recent_days,
        low_volume_percentile=args.low_volume_percentile,
        min_hit_days=args.low_absorb_min_hit_days,
    )
    near_ma20 = abs(close / ma20 - 1) * 100 <= args.pullback_near_ma_pct
    near_ma60 = abs(close / ma60 - 1) * 100 <= args.pullback_near_ma_pct * 1.5
    pullback_depth_ok = args.pullback_min_drawdown_pct <= abs(drawdown_120d_pct) <= args.pullback_max_drawdown_pct
    pullback_flag = bool(close > ma60 and (near_ma20 or near_ma60) and pullback_depth_ok and low_absorb_ok)

    # 可选硬要求
    if args.require_bull_ma and not bull_ma_flag:
        return None
    if args.require_macd_positive and not macd_positive_flag:
        return None
    if args.require_breakout_or_low_absorb and not (breakout_flag or pullback_flag):
        return None

    # 基本面硬要求：只有提供 Tushare 且字段有效时才严格执行
    roe = fina.get("roe")
    revenue_yoy = fina.get("revenue_yoy")
    netprofit_yoy = fina.get("netprofit_yoy")
    debt_to_assets = fina.get("debt_to_assets")
    if args.tushare_token:
        if roe is not None and not math.isnan(roe) and roe < args.min_roe:
            return None
        if revenue_yoy is not None and not math.isnan(revenue_yoy) and revenue_yoy < args.min_revenue_yoy:
            return None
        if netprofit_yoy is not None and not math.isnan(netprofit_yoy) and netprofit_yoy < args.min_netprofit_yoy:
            return None
        if debt_to_assets is not None and not math.isnan(debt_to_assets) and debt_to_assets > args.max_debt_to_assets:
            return None

    score = 0.0
    reasons: List[str] = []

    # 1) 质量/估值 25 分
    if total_mv_yi >= args.min_market_cap_yi * 2:
        score += 5; reasons.append("市值充足")
    if avg_amount20_yi >= args.min_avg_amount_yi * 1.5:
        score += 5; reasons.append("流动性较好")
    if 5 <= pe <= args.good_pe:
        score += 5; reasons.append("PE合理")
    if 0.8 <= pb <= args.good_pb:
        score += 5; reasons.append("PB合理")
    if args.tushare_token:
        if roe is not None and not math.isnan(roe) and roe >= args.min_roe:
            score += 5; reasons.append(f"ROE达标{roe:.1f}%")
        if revenue_yoy is not None and not math.isnan(revenue_yoy) and revenue_yoy >= args.min_revenue_yoy:
            score += 5; reasons.append(f"营收增长{revenue_yoy:.1f}%")
        if netprofit_yoy is not None and not math.isnan(netprofit_yoy) and netprofit_yoy >= args.min_netprofit_yoy:
            score += 5; reasons.append(f"净利增长{netprofit_yoy:.1f}%")
        if debt_to_assets is not None and not math.isnan(debt_to_assets) and debt_to_assets <= args.max_debt_to_assets:
            score += 3; reasons.append(f"负债率可控{debt_to_assets:.1f}%")

    # 2) 趋势 30 分
    if bull_ma_flag:
        score += 10; reasons.append("MA20>MA60且站上MA20")
    if strong_bull_flag:
        score += 8; reasons.append("短中期均线多头")
    if macd_positive_flag:
        score += 6; reasons.append("MACD零轴上方强势")
    elif dif > dea:
        score += 4; reasons.append("MACD金叉/多头")
    if macd_golden_recent or macd_improving_flag:
        score += 4; reasons.append("MACD近期转强")
    if safe_float(last.get("ret_60d")) > 0:
        score += 2; reasons.append("60日收益为正")

    # 3) 量价/形态 30 分
    if breakout_flag:
        score += 12; reasons.append("接近/突破60日高点并放量")
    if pullback_flag:
        score += 12; reasons.append("趋势内缩量回踩低吸")
    if args.volume_ratio_low <= volume_ratio_5 <= args.volume_ratio_high:
        score += 5; reasons.append(f"量比适中{volume_ratio_5:.2f}")
    if close_from_120d_low_pct >= args.min_from_120d_low_pct:
        score += 4; reasons.append("脱离120日低点")
    if drawdown_120d_pct >= -args.max_drawdown_from_120d_high_pct:
        score += 4; reasons.append("距120日高点不远")
    if low_absorb_days >= args.low_absorb_min_hit_days:
        score += 5; reasons.append(f"近{args.low_absorb_recent_days}天缩量阴/弱K {low_absorb_days}次")

    # 4) 风险扣分
    risk_penalty = 0.0
    if close_from_120d_low_pct > args.max_from_120d_low_pct:
        risk_penalty += 8; reasons.append("扣分：涨幅过大")
    if abs(drawdown_120d_pct) > args.hard_max_drawdown_from_120d_high_pct:
        risk_penalty += 8; reasons.append("扣分：离高点过远")
    if volume_ratio_5 > args.abnormal_volume_ratio:
        risk_penalty += 5; reasons.append("扣分：异常爆量")
    if pct_chg >= 9.5:
        risk_penalty += 4; reasons.append("扣分：当日接近涨停，不追高")

    score = max(0.0, min(100.0, score - risk_penalty))

    if score < args.min_score:
        return None

    if breakout_flag and pullback_flag:
        pattern = "突破+低吸共振"
    elif breakout_flag:
        pattern = "趋势突破"
    elif pullback_flag:
        pattern = "缩量回踩低吸"
    elif bull_ma_flag:
        pattern = "趋势持有"
    else:
        pattern = "观察"

    return ScreenResult(
        code=code,
        name=name,
        close=round(close, 3),
        pct_chg=round(pct_chg, 3),
        total_mv_yi=round(total_mv_yi, 2),
        amount_yi=round(amount_yi, 2),
        avg_amount20_yi=round(avg_amount20_yi, 2),
        pe=round(pe, 3),
        pb=round(pb, 3),
        score=round(score, 2),
        grade=grade_from_score(score),
        pattern=pattern,
        reasons=" | ".join(reasons[:12]),
        ma5=round(ma5, 3),
        ma10=round(ma10, 3),
        ma20=round(ma20, 3),
        ma60=round(ma60, 3),
        ma120=round(ma120, 3),
        dif=round(dif, 4),
        dea=round(dea, 4),
        macd_hist=round(macd_hist, 4),
        volume_ratio_5=round(volume_ratio_5, 3),
        volume_ratio_20=round(volume_ratio_20, 3),
        close_to_60d_high_pct=round(close_to_60d_high_pct, 3),
        close_from_120d_low_pct=round(close_from_120d_low_pct, 3),
        drawdown_120d_pct=round(drawdown_120d_pct, 3),
        low_absorb_days=int(low_absorb_days),
        breakout_flag=bool(breakout_flag),
        pullback_flag=bool(pullback_flag),
        bull_ma_flag=bool(bull_ma_flag),
        macd_positive_flag=bool(macd_positive_flag),
        roe=None if roe is None or math.isnan(roe) else round(float(roe), 3),
        revenue_yoy=None if revenue_yoy is None or math.isnan(revenue_yoy) else round(float(revenue_yoy), 3),
        netprofit_yoy=None if netprofit_yoy is None or math.isnan(netprofit_yoy) else round(float(netprofit_yoy), 3),
        debt_to_assets=None if debt_to_assets is None or math.isnan(debt_to_assets) else round(float(debt_to_assets), 3),
    )


def process_one(row_dict: Dict, args: argparse.Namespace, start_date: str, end_date: str) -> Optional[ScreenResult]:
    row = pd.Series(row_dict)
    code = str(row.get("code", "")).zfill(6)
    try:
        hist = load_hist_from_cache(code, args, start_date, end_date)
        # Tushare pro 对象不适合在线程之间直接共享，这里每只股票临时取；为减少慢速，默认不开启
        fina = {"roe": None, "revenue_yoy": None, "netprofit_yoy": None, "debt_to_assets": None}
        if args.tushare_token:
            # 财务近两年即可
            f_start = (datetime.now() - timedelta(days=800)).strftime("%Y%m%d")
            fetcher = TushareFundamentalFetcher(args.tushare_token, f_start, end_date)
            fina = fetcher.fetch(code)
        return score_one_stock(row, hist, fina, args)
    except Exception:
        return None


# ---------------------------
# CLI
# ---------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="A股：优质基本面 + 趋势突破/缩量回踩低吸筛选器")

    # universe and hard filters
    p.add_argument("--main-board-only", action="store_true", default=True, help="只筛沪深主板/中小板，默认开启")
    p.add_argument("--include-non-main-board", dest="main_board_only", action="store_false", help="包含创业板/科创板/北交所")
    p.add_argument("--min-market-cap-yi", type=float, default=80, help="最低总市值，单位亿元")
    p.add_argument("--min-avg-amount-yi", type=float, default=1.0, help="近20日最低平均成交额，单位亿元")
    p.add_argument("--min-price", type=float, default=3.0)
    p.add_argument("--max-price", type=float, default=200.0)
    p.add_argument("--max-pe", type=float, default=80.0)
    p.add_argument("--max-pb", type=float, default=12.0)
    p.add_argument("--good-pe", type=float, default=35.0, help="评分用合理PE上限")
    p.add_argument("--good-pb", type=float, default=5.0, help="评分用合理PB上限")

    # optional fundamentals
    p.add_argument("--tushare-token", type=str, default=os.getenv("TUSHARE_TOKEN", ""), help="可选：Tushare Token")
    p.add_argument("--min-roe", type=float, default=8.0, help="Tushare基本面：最低ROE百分比")
    p.add_argument("--min-revenue-yoy", type=float, default=5.0, help="Tushare基本面：最低营收同比百分比")
    p.add_argument("--min-netprofit-yoy", type=float, default=5.0, help="Tushare基本面：最低净利同比百分比")
    p.add_argument("--max-debt-to-assets", type=float, default=75.0, help="Tushare基本面：最高资产负债率百分比")

    # technical thresholds
    p.add_argument("--ma120-tolerance-pct", type=float, default=3.0, help="MA60相对MA120允许下穿容忍")
    p.add_argument("--breakout-near-pct", type=float, default=2.0, help="距离60日高点多少百分比内视为近突破")
    p.add_argument("--breakout-min-volume-ratio", type=float, default=1.35)
    p.add_argument("--breakout-min-pct-chg", type=float, default=1.0)
    p.add_argument("--volume-ratio-low", type=float, default=0.8)
    p.add_argument("--volume-ratio-high", type=float, default=3.5)
    p.add_argument("--abnormal-volume-ratio", type=float, default=5.0)

    # low absorb / pullback
    p.add_argument("--low-absorb-lookback-days", type=int, default=240)
    p.add_argument("--low-absorb-recent-days", type=int, default=10)
    p.add_argument("--low-volume-percentile", type=float, default=15.0)
    p.add_argument("--low-absorb-min-hit-days", type=int, default=2)
    p.add_argument("--pullback-near-ma-pct", type=float, default=5.0)
    p.add_argument("--pullback-min-drawdown-pct", type=float, default=5.0)
    p.add_argument("--pullback-max-drawdown-pct", type=float, default=28.0)

    # position/risk
    p.add_argument("--min-from-120d-low-pct", type=float, default=10.0)
    p.add_argument("--max-from-120d-low-pct", type=float, default=180.0)
    p.add_argument("--max-drawdown-from-120d-high-pct", type=float, default=25.0)
    p.add_argument("--hard-max-drawdown-from-120d-high-pct", type=float, default=40.0)

    # strict switches
    p.add_argument("--require-bull-ma", action="store_true", help="必须均线趋势达标")
    p.add_argument("--require-macd-positive", action="store_true", help="必须MACD零轴上方多头")
    p.add_argument("--require-breakout-or-low-absorb", action="store_true", help="必须是突破或缩量回踩低吸形态")

    # runtime
    p.add_argument("--min-score", type=float, default=65.0)
    p.add_argument("--workers", type=int, default=4, help="更新日线缓存并发线程数")
    p.add_argument("--screen-workers", type=int, default=8, help="筛选计算并发线程数，默认 8")
    p.add_argument("--limit", type=int, default=0, help="调试用，只处理前N只")
    p.add_argument("--history-days", type=int, default=430)
    p.add_argument("--adjust", type=str, default="qfq", choices=["", "qfq", "hfq"], help="复权方式：qfq前复权，hfq后复权，空为不复权")
    p.add_argument("--spot-retry", type=int, default=5, help="实时行情接口重试次数")
    p.add_argument("--spot-retry-sleep", type=float, default=1.5, help="实时行情接口失败后的递增等待秒数")
    p.add_argument("--data-dir", type=str, default=str(DATA_DIR), help="数据缓存目录，默认 data")
    p.add_argument("--candidate-output", type=str, default=str(CANDIDATE_TABLE_PATH), help="候选表缓存路径")
    p.add_argument("--refresh-candidates", action="store_true", help="强制刷新候选表缓存")
    p.add_argument("--skip-kline-update", action="store_true", help="只使用本地K线缓存，不联网更新")
    p.add_argument("--force-kline-update", action="store_true", help="忽略本地缓存，强制重新拉取历史日线")
    p.add_argument("--include-today", action="store_true", help="未指定 end-date 时强制包含今日")
    p.add_argument("--exclude-today", action="store_true", help="未指定 end-date 时强制不包含今日")
    p.add_argument("--end-date", type=str, default=None, help="统计截止日期 YYYYMMDD")
    p.add_argument("--output", type=str, default="screen_result.csv")
    return p


def main() -> int:
    args = build_parser().parse_args()
    args.data_dir_path = Path(args.data_dir).resolve()
    args.kline_dir_path = args.data_dir_path / "kline"
    args.candidate_output_path = Path(args.candidate_output)
    if not args.candidate_output_path.is_absolute():
        args.candidate_output_path = (PROJECT_DIR / args.candidate_output_path).resolve()
    args.effective_end_date = resolve_effective_end_date(args)
    start_date, end_date = history_window(args.effective_end_date, args.history_days)
    args.data_dir_path.mkdir(parents=True, exist_ok=True)
    args.kline_dir_path.mkdir(parents=True, exist_ok=True)

    spot = load_or_refresh_candidates(args)

    # 基础过滤尽量前置，减少历史K线请求
    spot = spot[spot["code"].apply(lambda x: is_main_board(str(x)) if args.main_board_only else True)]
    spot = spot[~spot["name"].apply(is_risky_name)]
    if "total_mv" in spot.columns:
        total_mv = pd.to_numeric(spot["total_mv"], errors="coerce")
        if total_mv.notna().any():
            spot = spot[total_mv / 1e8 >= args.min_market_cap_yi]
        else:
            print("[WARN] 当前行情源不含 total_mv，已跳过市值过滤。")
    if "amount" in spot.columns:
        amount = pd.to_numeric(spot["amount"], errors="coerce")
        if amount.notna().any():
            spot = spot[amount / 1e8 >= max(0.2, args.min_avg_amount_yi * 0.3)]
        else:
            print("[WARN] 当前行情源不含 amount，已跳过成交额过滤。")
    if "pe" in spot.columns:
        pe = pd.to_numeric(spot["pe"], errors="coerce")
        if pe.notna().any():
            spot = spot[(pe > 0) & (pe <= args.max_pe)]
        else:
            print("[WARN] 当前行情源不含 PE，已跳过 PE 过滤。")
    if "pb" in spot.columns:
        pb = pd.to_numeric(spot["pb"], errors="coerce")
        if pb.notna().any():
            spot = spot[(pb > 0) & (pb <= args.max_pb)]
        else:
            print("[WARN] 当前行情源不含 PB，已跳过 PB 过滤。")

    spot = spot.reset_index(drop=True)
    if args.limit and args.limit > 0:
        spot = spot.head(args.limit)

    print(f"候选基础池：{len(spot)} 只。先更新本地日线缓存，区间 {start_date} ~ {end_date}。")
    rows = spot.to_dict("records")
    update_kline_cache(args, rows)

    print(f"开始基于本地缓存评分，区间 {start_date} ~ {end_date}。")
    results: List[ScreenResult] = []

    iterator = rows
    if tqdm is not None:
        iterator = tqdm(rows, desc="screening")

    if args.screen_workers <= 1:
        for r in iterator:
            out = process_one(r, args, start_date, end_date)
            if out:
                results.append(out)
    else:
        with cf.ThreadPoolExecutor(max_workers=args.screen_workers) as ex:
            futs = [ex.submit(process_one, r, args, start_date, end_date) for r in rows]
            iter_futs = cf.as_completed(futs)
            if tqdm is not None:
                iter_futs = tqdm(iter_futs, total=len(futs), desc="screening")
            for fut in iter_futs:
                out = fut.result()
                if out:
                    results.append(out)

    if not results:
        print("没有股票满足条件。可降低 --min-score 或放宽市值/成交额/形态硬要求。")
        return 0

    out_df = pd.DataFrame([asdict(x) for x in results])
    out_df = out_df.sort_values(["score", "total_mv_yi", "avg_amount20_yi"], ascending=[False, False, False])

    output, output_xlsx, archive_csv, archive_xlsx = write_outputs_with_archive(out_df, args)
    write_run_state(args, len(rows), len(results), output, archive_csv)

    cols = [
        "code", "name", "score", "grade", "pattern", "close", "pct_chg",
        "total_mv_yi", "avg_amount20_yi", "pe", "pb",
        "volume_ratio_5", "close_to_60d_high_pct", "drawdown_120d_pct",
        "low_absorb_days", "reasons",
    ]
    print("\n筛选结果 Top 50：")
    print(out_df[cols].head(50).to_string(index=False))
    print(f"\n已保存：{output.resolve()}")
    if output_xlsx is not None and output_xlsx.exists():
        print(f"已保存：{output_xlsx.resolve()}")
    print(f"已归档：{archive_csv.resolve()}")
    if archive_xlsx is not None and archive_xlsx.exists():
        print(f"已归档：{archive_xlsx.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
