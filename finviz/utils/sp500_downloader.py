import pandas as pd

# Download the table from Wikipedia
url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
if __name__ == '__main__':
    tables = pd.read_html(url)

    # The first table contains the tickers
    sp500_table = tables[0]

    # Get the tickers as a list
    tickers = sp500_table['Symbol'].tolist()
    print(tickers)
