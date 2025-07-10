import os
import tempfile

import pytest

from investment.utils.finviz_scraper import scrape_finviz, scrape_to_file
from investment.utils.finviz_views import FinVizView


@pytest.mark.parametrize("view_type", list(FinVizView))
def test_scrape_basic(view_type):
    size = 10
    df = scrape_finviz(view_type, length=size)
    assert df is not None
    assert not df.empty
    assert len(df) == size
    assert len(df.columns) == len(view_type.columns())


@pytest.mark.parametrize("offset", [10, 100, 1_000, 10_000])
def test_scrape_offset(offset):
    size = 10
    df = scrape_finviz(FinVizView.TECHNICAL, offset=offset, length=size)
    assert df is not None
    assert not df.empty
    assert len(df) == size
    assert df._ixs(0)["No."] == str(offset)


def test_scrape_more_than_100_000():
    size = 10
    offset = 100_000
    df = scrape_finviz(FinVizView.TECHNICAL, offset=offset, length=size)
    assert df is not None
    assert len(df) == 1


def test_save_io():
    size = 10
    view_type = FinVizView.OWNERSHIP
    with tempfile.NamedTemporaryFile(suffix='.parquet', mode="w+", delete=True) as tmp:
        output_file = scrape_to_file(view_type, length=size, filename=tmp.name)
        assert os.path.isfile(output_file)
