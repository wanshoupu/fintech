import os

import pandas as pd

from investment.utils.finviz_formatter import clean_finviz
from pathlib import Path


def test_finviz_formatter():
    parent_dir = Path(__file__).resolve().parent.parent
    data_file = os.path.abspath(os.path.join(parent_dir, 'data', 'finviz-ALL-1-10220.parquet'))
    df = pd.read_parquet(data_file)
    df = clean_finviz(df)

    # print(df.columns.tolist())
    types = df.dtypes.describe()
    # print(types)
    assert types['count'] == df.shape[1]
    assert types['unique'] == 11
    assert types['top'] == 'float64'
    assert types['freq'] == 104
