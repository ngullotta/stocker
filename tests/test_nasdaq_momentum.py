import sys

sys.argv.insert(1, "--window=120")

from nasdaq_100_momentum import mtl


def test_df():
    assert mtl is not None