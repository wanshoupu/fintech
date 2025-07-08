import asyncio
import datetime

from investment.utils.finviz_scraper import scrape_to_file
from investment.utils.finviz_views import FinVizView

date = datetime.datetime.today()
filename = f'{date.year}-{date.month}-{date.day}'
asyncio.run(scrape_to_file(FinVizView.ALL, start_offset=1, base_filename=filename))
