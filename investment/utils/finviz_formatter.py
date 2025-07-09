import re

import numpy as np
import pandas as pd


def parse_abbreviated_number(s):
    if pd.isna(s):
        return np.nan

    s = s.upper().replace(',', '')
    match = re.match(r'^(-?[\d\.]+)([KMBT]?)$', s)
    if not match:
        return np.nan

    num, suffix = match.groups()
    num = float(num)
    multiplier = {
        '': 1,
        'K': 1e3,
        'M': 1e6,
        'B': 1e9,
        'T': 1e12,
    }.get(suffix, np.nan)
    return num * multiplier


def parse_percentage(s):
    try:
        return float(s.replace('%', '')) / 100
    except:
        return np.nan


def parse_datetime(series: pd.Series) -> pd.Series | None:
    for fmt in SUPPORTED_DATETIME_FORMATS:
        try:
            return pd.to_datetime(series, format=fmt, errors='raise')
        except Exception:
            continue
    return None


SUPPORTED_DATETIME_FORMATS = [
    "%m/%d/%Y",
    "%Y/%m/%d"
    "%Y-%m-%d",
    "%b-%d-%Y",
    "%d-%b-%Y",
]


def parse_52w_range(range_str: str):
    if pd.isna(range_str):
        return (np.nan, np.nan)

    # Match numeric range pattern
    match = re.match(r'^([\d\.]+)\s*-\s*([\d\.]+)$', range_str)
    if match:
        low, high = match.groups()
        return (float(low), float(high))

    return (np.nan, np.nan)


def smart_convert(s: pd.Series) -> pd.Series:
    s = s.replace('-', pd.NA)

    # Try datetime
    dt = parse_datetime(s)
    if dt is not None:
        return dt

    # Try boolean
    if set(s.dropna().str.lower().unique()) <= {'true', 'false', 'yes', 'no'}:
        return s.str.lower().map({'true': True, 'false': False,
                                  'yes': True, 'no': False})
    # Try category
    if s.nunique(dropna=True) < 50:
        return s.astype('category')

    # Percent
    if s.str.endswith('%').all():
        return s.apply(parse_percentage)

    s_clean = s.str.replace(',', '', regex=False)

    # Handle abbreviated numbers (K/M/B)
    if s_clean.str.match(r'^-?[\d\.]+[KMBT]$', case=False).all():
        return s_clean.apply(parse_abbreviated_number)

    # Try integer
    try:
        return pd.to_numeric(s_clean, errors='raise', downcast='integer')
    except:
        pass

    # Try float
    try:
        return pd.to_numeric(s_clean, errors='raise', downcast='float')
    except:
        pass

    return s  # fallback


def clean_finviz(df: pd.DataFrame) -> pd.DataFrame:
    formatted = df.apply(smart_convert)
    formatted['Ticker'] = formatted['Ticker'].astype('string')
    formatted[['52W_Low', '52W_High']] = formatted['52W Range'].apply(lambda x: pd.Series(parse_52w_range(x)))
    del formatted['52W Range']
    del formatted['No.']
    return formatted.select_dtypes(exclude=['object'])
