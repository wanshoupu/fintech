from investment.utils.headers import patch_dup_headers


def test_patch_one_dup():
    headers = ['No.', 'Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Fwd P/E', 'PEG', 'P/S', 'P/B', 'P/C', 'P/FCF', 'Dividend', 'Payout Ratio', 'EPS', 'EPS This Y',
               'EPS Next Y', 'EPS Past 5Y', 'EPS Next 5Y', 'Sales Past 5Y', 'EPS Q/Q', 'Sales Q/Q', 'Outstanding', 'Float', 'Insider Own', 'Insider Trans', 'Inst Own', 'Inst Trans', 'Short Float',
               'Short Ratio', 'ROA', 'ROE', 'ROIC', 'Curr R', 'Quick R', 'LTDebt/Eq', 'Debt/Eq', 'Gross M', 'Oper M', 'Profit M', 'Perf Week', 'Perf Month', 'Perf Quart', 'Perf Half', 'Perf Year',
               'Perf YTD', 'Beta', 'ATR', 'Volatility W', 'Volatility M', 'SMA20', 'SMA50', 'SMA200', '50D High', '50D Low', '52W High', '52W Low', 'RSI', 'Change from Open', 'Gap', 'Recom',
               'Avg Volume', 'Rel Volume', 'Price', 'Change', 'Volume', 'Earnings', 'Target Price', 'IPO Date', 'Book/sh', 'Cash/sh', 'Dividend', 'Employees', 'EPS next Q', 'Income', 'Index',
               'Optionable', 'Prev Close', 'Sales', 'Shortable', 'Short Interest', 'Float %', 'Open', 'High', 'Low', 'Asset Type', 'Single Category', 'Tags', 'Expense', 'Holdings', 'AUM', 'Flows 1M',
               'Flows% 1M', 'Flows 3M', 'Flows% 3M', 'Flows YTD', 'Flows% YTD', 'Return% 1Y', 'Return% 3Y', 'Return% 5Y', 'All-Time High', 'All-Time Low', 'EPS Surprise', 'Revenue Surprise',
               'Exchange', 'Dividend TTM', 'Dividend Ex Date', 'EPS YoY TTM', 'Sales YoY TTM', '52W Range', 'News Time', 'News URL', 'News Title', 'Perf 3Y', 'Perf 5Y', 'Perf 10Y', 'EPS Past 3Y',
               'Sales Past 3Y', 'Enterprise Value', 'EV/EBITDA', 'EV/Sales', 'Dividend Gr. 1Y', 'Dividend Gr. 3Y', 'Dividend Gr. 5Y']
    uniq_headers = patch_dup_headers(headers)
    assert len(uniq_headers) == len(set(uniq_headers))


def test_patch_more_duplicates():
    headers = ['No.', 'Ticker', 'Company', 'PEG', 'No.', 'Ticker', 'Company', 'PEG', 'Company', 'PEG', 'Sector']
    uniq_headers = patch_dup_headers(headers)
    assert len(uniq_headers) == len(set(uniq_headers))
    print(uniq_headers)
