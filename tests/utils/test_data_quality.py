import os
from pathlib import Path

import pandas as pd

from investment.utils.data_quality import na_hist


def test_na_hist_normal():
    parent_dir = Path(__file__).resolve().parent.parent
    data_file = os.path.abspath(os.path.join(parent_dir, 'data', 'finviz-ALL-1-10220.parquet'))
    df = pd.read_parquet(data_file)
    cols, hist = na_hist(df)

    # assert cols are sorted
    assert set(cols) == set(df.columns.tolist())


def test_na_hist_nona():
    parent_dir = Path(__file__).resolve().parent.parent
    data_file = os.path.abspath(os.path.join(parent_dir, 'data', 'bluechip-data.parquet'))
    df = pd.read_parquet(data_file)
    cols, hist = na_hist(df)

    # assert cols are sorted
    assert set(cols) == set(df.columns.tolist())
    assert len(hist) > 0
