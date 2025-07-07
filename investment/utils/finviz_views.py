from collections import namedtuple
from enum import Enum


class FinVizView(Enum):
    OVERVIEW = ("v=111", ("No.", "Ticker", "Company", "Sector", "Industry", "Country", "Market Cap", "P/E", "Price", "Change", "Volume"))
    VALUATION = ("v=121",
                 ('No.', 'Ticker', 'Market Cap', 'P/E', 'Fwd P/E', 'PEG', 'P/S', 'P/B', 'P/C', 'P/FCF', 'EPS This Y', 'EPS Next Y', 'EPS Past 5Y', 'EPS Next 5Y', 'Sales Past 5Y', 'Price', 'Change',
                  'Volume'))
    FINANCIAL = ("v=161",
                 ('No.', 'Ticker', 'Market Cap', 'Dividend', 'ROA', 'ROE', 'ROIC', 'Curr R', 'Quick R', 'LTDebt/Eq', 'Debt/Eq', 'Gross M', 'Oper M', 'Profit M', 'Earnings', 'Price', 'Change',
                  'Volume'))
    OWNERSHIP = ("v=131",
                 ('No.', 'Ticker', 'Market Cap', 'Outstanding', 'Float', 'Insider Own', 'Insider Trans', 'Inst Own', 'Inst Trans', 'Short Float', 'Short Ratio', 'Avg Volume', 'Price', 'Change',
                  'Volume'))
    PERFORMANCE = ("v=141",
                   ('No.', 'Ticker', 'Perf Week', 'Perf Month', 'Perf Quart', 'Perf Half', 'Perf YTD', 'Perf Year', 'Perf 3Y', 'Perf 5Y', 'Perf 10Y', 'Volatility W', 'Volatility M', 'Avg Volume',
                    'Rel Volume', 'Price', 'Change', 'Volume'))
    TECHNICAL = ("v=171",
                 ('No.', 'Ticker', 'Beta', 'ATR', 'SMA20', 'SMA50', 'SMA200', '52W High', '52W Low', 'RSI', 'Price', 'Change', 'Change from Open', 'Gap', 'Volume'))
    ALL = (
        "v=152&c=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149",
        ('No.', 'Ticker', 'Company', 'Sector', 'Industry', 'Country', 'Market Cap', 'P/E', 'Fwd P/E', 'PEG', 'P/S', 'P/B', 'P/C', 'P/FCF', 'Dividend', 'Payout Ratio', 'EPS', 'EPS This Y',
         'EPS Next Y', 'EPS Past 5Y', 'EPS Next 5Y', 'Sales Past 5Y', 'EPS Q/Q', 'Sales Q/Q', 'Outstanding', 'Float', 'Insider Own', 'Insider Trans', 'Inst Own', 'Inst Trans', 'Short Float',
         'Short Ratio', 'ROA', 'ROE', 'ROIC', 'Curr R', 'Quick R', 'LTDebt/Eq', 'Debt/Eq', 'Gross M', 'Oper M', 'Profit M', 'Perf Week', 'Perf Month', 'Perf Quart', 'Perf Half', 'Perf Year',
         'Perf YTD', 'Beta', 'ATR', 'Volatility W', 'Volatility M', 'SMA20', 'SMA50', 'SMA200', '50D High', '50D Low', '52W High', '52W Low', 'RSI', 'Change from Open', 'Gap', 'Recom', 'Avg Volume',
         'Rel Volume', 'Price', 'Change', 'Volume', 'Earnings', 'Target Price', 'IPO Date', 'Book/sh', 'Cash/sh', 'Dividend', 'Employees', 'EPS next Q', 'Income', 'Index', 'Optionable', 'Prev Close',
         'Sales', 'Shortable', 'Short Interest', 'Float %', 'Open', 'High', 'Low', 'Asset Type', 'Single Category', 'Tags', 'Expense', 'Holdings', 'AUM', 'Flows 1M', 'Flows% 1M', 'Flows 3M',
         'Flows% 3M', 'Flows YTD', 'Flows% YTD', 'Return% 1Y', 'Return% 3Y', 'Return% 5Y', 'All-Time High', 'All-Time Low', 'EPS Surprise', 'Revenue Surprise', 'Exchange', 'Dividend TTM',
         'Dividend Ex Date', 'EPS YoY TTM', 'Sales YoY TTM', '52W Range', 'News Time', 'News URL', 'News Title', 'Perf 3Y', 'Perf 5Y', 'Perf 10Y', 'EPS Past 3Y', 'Sales Past 3Y', 'Enterprise Value',
         'EV/EBITDA', 'EV/Sales', 'Dividend Gr. 1Y', 'Dividend Gr. 3Y', 'Dividend Gr. 5Y'))

    def view_id(self):
        return self.value[0]

    def columns(self):
        return self.value[1]

    def url_template(self) -> str:
        return "https://finviz.com/screener.ashx?" + self.view_id() + "&r={offset}"

    def __repr__(self):
        if self.name:
            return self.name
        return repr(int(self))


class FinvizUrl:
    def __init__(self):
        self.base = 'https://finviz.com/screener.ashx'


TableData = namedtuple(
    "TableData", [
        "headers",
        "rows",
    ])
