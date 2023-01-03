import sys
from datetime import datetime

then = datetime(year=2009, month=1, day=1)
now = datetime.now()
window = (now.year - then.year) * 12 + now.month - then.month
sys.argv.insert(1, "--window=%d" % window)

from numpy import prod
from pandas import Series, isna
from yfinance import download

from nasdaq_100_momentum import mtl


def test_df():
    twelve = mtl.rolling(12).apply(prod)
    six = mtl.rolling(6).apply(prod)
    three = mtl.rolling(3).apply(prod)

    returns = []
    for date in mtl.index[12:-1]:
        indices = None
        for df, number in [
            (twelve, 50),
            (six, 30),
            (three, 10),
        ]:
            if indices is not None:
                top = df.loc[date, indices].nlargest(number)
            else:
                top = df.loc[date].nlargest(number)
            indices = top.index
        portfolio = mtl.loc[date:, indices][1:2]
        performance = portfolio.mean(axis=1).values[0]
        returns.append(performance)

    nasdaq = download("^IXIC", start=then.strftime("%Y-%m-%d"))
    strat_returns = Series(
        [i - 0.005 for i in returns], index=mtl.index[12:-1]
    ).cumprod()
    nasdaq_returns = (nasdaq["Adj Close"].pct_change() + 1).cumprod()

    for sr, nr in zip(strat_returns.to_list(), nasdaq_returns.to_list()):
        assert not isna(sr)
        if not isna(nr):
            assert sr > nr
