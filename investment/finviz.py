import os
from pathlib import Path
import pandas as pd
from pandasgui import show
from investment.utils.data_quality import na_hist, bluechips
from investment.analysis.viz import na_viz
from investment.utils.finviz_formatter import clean_finviz

if __name__ == '__main__':
    parent_dir = Path(__file__).resolve().parent.parent
    data_file = os.path.abspath(os.path.join(parent_dir, 'tests', 'data', 'finviz-ALL-1-10220.parquet'))
    df = pd.read_parquet(data_file)

    # cols, hist = na_hist(df)
    # na_viz(hist)

    df = clean_finviz(df)
    df = bluechips(df)
    show(df)
