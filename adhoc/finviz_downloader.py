import argparse

from investment.utils.finviz_scraper import scrape_to_file
from investment.utils.finviz_views import FinVizView

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--view", help="The source to parse, if any", default="ALL")
    parser.add_argument("-s", "--start", help="The offset to start at, if any", default=1, type=int)
    parser.add_argument("-l", "--length", help="The length limit of the scaper, if any", type=int)
    args = parser.parse_args()

    view_type = FinVizView[args.view]
    start = args.start
    length = args.length
    fn = scrape_to_file(view_type, offset=start, length=length)
    print(fn)
