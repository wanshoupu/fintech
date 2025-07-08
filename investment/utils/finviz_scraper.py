import pandas as pd
from playwright.async_api import async_playwright

from investment.utils.finviz_views import TableData, FinVizView
from investment.utils.headers import patch_dup_headers


async def scrape_to_file(view_type: FinVizView, start_offset: int = 1, length: int = None, base_filename: str = None) -> None:
    """
    Scrape finviz stocks data.
    :param view_type: FinVizView type.
    :param start_offset: Start offset must be >= 1 and defaults to 1.
    If provided, will scrape starting from that number inclusive.
    :param length: total number of records to scrape.
    If set, will return when length of records are scraped or end of records reached.
    Default to None for as long as the records are available.
    :param base_filename: Base filename without extension.
    For example, if "mydata" is given, the output files will be "mydata.csv" and "mydata.parquet".
    """
    df = await scrape_finviz(view_type, start_offset=start_offset, length=length)

    if df.empty:
        print("No data scraped.")
        return
    print(df.head())
    base_filename = base_filename or f'finviz-{view_type.name}-{start_offset}-{start_offset + len(df)}'
    csv_file = base_filename + '.csv'
    parquet_file = base_filename + '.parquet'
    df.to_csv(csv_file, index=False)
    df.to_parquet(parquet_file, index=False)


async def scrape_finviz(view_type: FinVizView, start_offset=1, length=None) -> pd.DataFrame:
    """
    Scrape finviz stocks data.
    :param view_type: FinVizView type.
    :param start_offset: Start offset must be >= 1 and default to 1.
    If provided, will scrape starting from that number inclusive.
    :param length: total number of records to scrape.
    If set, will return when the length of records are scraped or end of records reached.
    Default to None for as long as the records are available.
    """
    url = view_type.url_template()
    table_data = await _scrape(url, start_offset, length)

    if table_data.rows:
        df = pd.DataFrame(table_data.rows, columns=patch_dup_headers(table_data.headers))
    else:
        df = pd.DataFrame(columns=view_type.columns())
    return df


async def _scrape(url, start_offset, length=None) -> TableData:
    all_data = []
    scraped_ids = set()
    headers = None
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        )
        page = await context.new_page()
        while True:
            print(f"Scraped page starting at record {start_offset}")
            try:
                await page.goto(url.format(offset=start_offset), wait_until='domcontentloaded')
                await page.wait_for_selector('table.screener_table', timeout=15000)
            except Exception as e:
                print(f"Error scraping at record {start_offset} due to error: {e}")
                continue

            table = await page.query_selector('table.screener_table')
            rows = await table.query_selector_all('tr')
            table_data = await extract_rows(rows)
            headers = table_data.headers
            new_ids = {row[0] for row in table_data.rows}
            if new_ids & scraped_ids:
                break
            scraped_ids.update(new_ids)
            all_data.extend(table_data.rows)
            if length is not None and len(all_data) >= length:
                all_data = all_data[:length]
                break
            start_offset += len(table_data.rows)

    return TableData(headers=headers, rows=all_data)


async def extract_rows(rows) -> TableData:
    all_data = []
    header = await rows[0].query_selector_all('th')
    header_data = [(await col.inner_text()).strip() for col in header]
    field_num = len(header_data)

    for row in rows[1:]:  # skip header row
        cols = await row.query_selector_all('td')
        if len(cols) < field_num:
            continue  # skip malformed rows

        row_data = [(await col.inner_text()).strip() for col in cols[:field_num]]
        all_data.append(row_data)
    return TableData(headers=header_data, rows=all_data)
