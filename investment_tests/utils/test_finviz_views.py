import pytest

from investment.utils.finviz_views import FinVizView


@pytest.mark.parametrize("view, view_id, name, num_cols", [
    (FinVizView.OVERVIEW, "v=111", "OVERVIEW", 11),
    (FinVizView.VALUATION, "v=121", "VALUATION", 18),
    (FinVizView.FINANCIAL, "v=161", "FINANCIAL", 18),
    (FinVizView.OWNERSHIP, "v=131", "OWNERSHIP", 15),
    (FinVizView.PERFORMANCE, "v=141", "PERFORMANCE", 18),
    (FinVizView.TECHNICAL, "v=171", "TECHNICAL", 15),
])
def test_finviz_view_basic(view, view_id, name, num_cols):
    assert view.view_id() == view_id
    assert view.name == name
    assert len(view.columns()) == num_cols


def test_finviz_view_custom():
    view = FinVizView.ALL
    assert view.name == 'ALL'
    assert view.view_id() == "v=152&c=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,111,112,113,114,115,116,117,118,119,120,121,122,123,124,125,126,127,128,129,130,131,132,133,134,135,136,137,138,139,140,141,142,143,144,145,146,147,148,149"
    assert len(view.columns()) == 126
