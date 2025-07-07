import os
import tempfile

import pytest

from investment.utils.finviz_scraper import scrape_finviz, scrape_to_file
from investment.utils.finviz_views import FinVizView


@pytest.mark.asyncio
@pytest.mark.parametrize("view_type", list(FinVizView))
async def test_scrape_basic(view_type):
    size = 10
    df = await scrape_finviz(view_type, length=size)
    assert df is not None
    assert not df.empty
    assert len(df) == size
    assert df.columns.tolist() == list(view_type.columns())


@pytest.mark.asyncio
@pytest.mark.parametrize("offset", [10, 100, 1_000, 10_000])
async def test_scrape_start_offset(offset):
    size = 10
    df = await scrape_finviz(FinVizView.TECHNICAL, start_offset=offset, length=size)
    assert df is not None
    assert not df.empty
    assert len(df) == size
    assert df._ixs(0)["No."] == str(offset)


@pytest.mark.asyncio
async def test_scrape_more_than_100_000():
    size = 10
    offset = 100_000
    df = await scrape_finviz(FinVizView.TECHNICAL, start_offset=offset, length=size)
    assert df is not None
    assert not df.empty
    assert len(df) == 1
    assert df._ixs(0)['No.'] == '10207'


@pytest.mark.asyncio
async def test_save_io():
    size = 10
    view_type = FinVizView.OWNERSHIP
    with tempfile.NamedTemporaryFile(suffix='.parquet', mode="w+", delete=True) as tmp:
        base_name = os.path.splitext(tmp.name)[0]  # remove the .txt extension
        csv_file = f"{base_name}.csv"
        await scrape_to_file(view_type, length=size, base_filename=base_name)
        assert os.path.isfile(tmp.name)
        assert os.path.exists(csv_file)
        os.remove(csv_file)
