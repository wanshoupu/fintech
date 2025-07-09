from typing import Tuple, List

import pandas as pd


def na_hist(df: pd.DataFrame) -> Tuple[List[str], List[int]]:
    nas = df.isna().sum()

    # 2. Sort columns by increasing NA rate
    sorted_cols = nas.sort_values().index.tolist()

    # 3. Initialize tracking
    num_cols = len(sorted_cols)
    remaining_rows = []

    # 4. For each N (number of columns to drop from the end)
    for n in range(1, num_cols):  # includes dropping 0 to all columns
        cols_to_keep = sorted_cols[:n]  # keep only first (num_cols - n) columns
        df_subset = df[cols_to_keep].dropna()
        if df_subset.empty:
            break
        remaining_rows.append(len(df_subset))
    return sorted_cols, remaining_rows


def bluechips(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.Country == 'USA']
    cols, hist = na_hist(df)
    if not hist:
        return df.dropna()
    num_row = hist[0] // 2
    k = next((i for i in range(len(hist)) if hist[i] <= num_row), 0)
    print(k, num_row, hist[k])
    return df[cols[:k]].dropna()
