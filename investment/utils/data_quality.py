from typing import Tuple, List

import pandas as pd


def na_hist(df: pd.DataFrame) -> Tuple[List[str], List[int]]:
    """
    Return a list of columns sorted by decreasing data quality (i.e., increasing number of missing values),
    along with the number of remaining rows after dropping rows with NA values, for each level of column removal.

    Returns:
        Tuple[List[str], List[int]]:
            - Column names sorted by increasing NA rate (highest quality first).
            - Corresponding list of remaining row counts after successively dropping columns with the most missing data.
    """
    nas = df.isna().sum()

    # 2. Sort columns by increasing NA rate
    cols_dcrs_quality = nas.sort_values().index.tolist()

    # 3. Initialize tracking
    num_cols = len(cols_dcrs_quality)
    remaining_rows = []

    # 4. For each N (number of columns to drop from the end)
    for n in range(1, 1 + num_cols):  # includes dropping 0 to all columns
        cols_to_keep = cols_dcrs_quality[:n]  # keep only first (num_cols - n) columns
        df_subset = df[cols_to_keep].dropna()
        if df_subset.empty:
            break
        remaining_rows.append(len(df_subset))
    remaining_rows.append(0)
    return cols_dcrs_quality, remaining_rows


def bluechips(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df.Country == 'USA']
    cols, hist = na_hist(df)
    if not hist:
        return df.dropna()
    num_row = hist[0] // 2
    k = next((i for i in range(len(hist)) if hist[i] <= num_row), 0)
    print(k, num_row, hist[k])
    return df[cols[:k]].dropna()
