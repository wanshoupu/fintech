import os

import pandas as pd

from investment.utils.finviz_formatter import clean_finviz
from pathlib import Path


def test_finviz_formatter():
    parent_dir = Path(__file__).resolve().parent.parent
    data_file = os.path.abspath(os.path.join(parent_dir, 'data', 'finviz-ALL-1-10220.parquet'))
    df = pd.read_parquet(data_file)
    formatted_df = clean_finviz(df)

    print(formatted_df.columns.tolist())
    types = formatted_df.dtypes.describe()
    assert types['count'] == formatted_df.shape[1]
    assert types['unique'] == 12
    assert types['top'] == 'float64'
    assert types['freq'] == 104
