import asyncio

from investment.utils.finviz_scraper import scrape_to_file
from investment.utils.finviz_views import FinVizView

if __name__ == '__main__':
    size = 10
    view_type = FinVizView.ALL

    start = 1
    asyncio.run(scrape_to_file(view_type, start_offset=start))
