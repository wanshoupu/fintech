import pandas as pd


if __name__ == '__main__':
    df = pd.read_parquet('/Users/shoupuwan/Downloads/finviz-ALL-1000-1010.parquet')
    print(df.head())
