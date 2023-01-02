import importlib

sandp = importlib.import_module("s&p_500_momentum")


def test_df():
    assert sandp.mtl is not None
