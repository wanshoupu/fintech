import pandas as pd
import requests
from bs4 import BeautifulSoup

from investment.utils.finviz_views import TableData, FinVizView
from investment.utils.headers import patch_dup_headers


def scrape_to_file(view_type: FinVizView, offset: int = 1, length: int = None, filename: str = None) -> str:
    """
    Scrape finviz stocks data.
    :param view_type: FinVizView type.
    :param offset: Start offset must be >= 1 and defaults to 1.
    If provided, will scrape starting from that number inclusive.
    :param length: total number of records to scrape.
    If set, will return when length of records are scraped or end of records reached.
    Default to None for as long as the records are available.
    :param filename: Base filename without extension.
    For example, if "mydata" is given, the output files will be "mydata.csv" and "mydata.parquet".
    """
    df = scrape_finviz(view_type, offset=offset, length=length)

    if df.empty:
        print("No data scraped.")
        return ''
    print(df.head())
    filename = filename or f'finviz-{view_type.name}-{offset}-{offset + len(df)}.parquet'
    df.to_parquet(filename, index=False)
    print(f'Wrote data to files: "{filename}"')
    return filename


def scrape_finviz(view_type: FinVizView, offset=1, length=None) -> pd.DataFrame:
    """
    Scrape finviz stocks data.
    :param view_type: FinVizView type.
    :param offset: Start offset must be >= 1 and default to 1.
    If provided, will scrape starting from that number inclusive.
    :param length: total number of records to scrape.
    If set, will return when the length of records are scraped or end of records reached.
    Default to None for as long as the records are available.
    """
    url = view_type.url_template()
    table_data = _scrape(url, offset, length)

    if table_data.rows:
        df = pd.DataFrame(table_data.rows, columns=patch_dup_headers(table_data.headers))
    else:
        df = pd.DataFrame(columns=view_type.columns())
    return df


def _scrape(url, offset, length=None) -> TableData:
    all_data = []
    scraped_ids = set()
    headers = None
    while True:
        print(f"Scraping page at record {offset}")
        try:
            table_data = extract_table(url.format(offset=offset))
        except Exception as e:
            print(f"Error scraping at record {offset} due to error: {e}")
            continue

        headers = table_data.headers
        new_ids = {row[0] for row in table_data.rows}
        if new_ids & scraped_ids:
            break
        scraped_ids.update(new_ids)
        all_data.extend(table_data.rows)
        if length is not None and len(all_data) >= length:
            all_data = all_data[:length]
            break
        offset += len(table_data.rows)

    return TableData(headers=headers, rows=all_data)


def extract_table(url) -> TableData:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
    }
    # Fetch HTML content
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise error if not 200
    soup = BeautifulSoup(response.text, "html.parser")
    # Find the table (you may need to adjust the selector)
    table = soup.find("table", class_="screener_table")
    if not table:
        raise RuntimeError("Could not find stock table on Finviz page")
    rows = []
    headers = []
    for i, tr in enumerate(table.find_all("tr")):
        cols = [td.text.strip() for td in tr.find_all(["td", "th"])]
        if i == 0:
            headers = cols
        else:
            rows.append(cols)
    return TableData(headers=headers, rows=rows)
