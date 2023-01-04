import logging
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
from sys import argv

from dateutil.relativedelta import relativedelta
from pandas import DataFrame, read_csv, read_html, to_datetime
from pandas.tseries.offsets import BDay
from yfinance import download

from stocker.cache import CacheController

# Don't run the whole script when refreshing tests
if "--cache-clear" in argv:
    pass
    # exit(0)

# `exit_on_error` and `parser.error` rewrite are for tests only. The
# argparser will choke if pytest params are passed in at runtime. Since
# we don't care, we will ignore them
parser = ArgumentParser(exit_on_error=False)
parser.error = lambda foo: None
config = ConfigParser()
parser.add_argument(
    "-w",
    "--window",
    type=int,
    default=12,
    help="Historical time period in months to go back for financial data",
)
parser.add_argument(
    "-p",
    "--period",
    type=int,
    default=10,
    help="RSI Period in days (don't count non-trading days e.g 2 weeks = 10)",
)
parser.add_argument(
    "-c",
    "--config",
    type=config.read,
    default="./config.cfg",
    help="Specify config file path",
)
parser.add_argument(
    "-l",
    "--log-level",
    type=logging.getLogger().setLevel,
    default="INFO",
    help="Specify log level",
)
args = parser.parse_args()

# Sanity checks
for section in ["s&p-500-rsi", "yfinance"]:
    failed = False
    if section not in config.sections():
        failed = True
        logging.error(f'[!] "{section}" not found in config')
        logging.error("[!] Please copy original config or fix content")
    if failed:
        exit(1)

# ---------------- Constants ----------------
now = datetime.now()
sconfig = config["s&p-500-rsi"]
yconfig = config["yfinance"]

# Stage 1
SANDP_500_LINK = sconfig.get("SANDP_500_LINK")
SANDP_500_TABLE_OFFSET = sconfig.getint("SANDP_500_TABLE_OFFSET")
SANDP_500_TABLE_SYMBOL_HEADER = sconfig.get("SANDP_500_TABLE_SYMBOL_HEADER")

# Stage 2
CLOSE_HEADER = yconfig.get("CLOSE_HEADER")
BEG_DT = (
    (now - relativedelta(months=args.window))
    .replace(day=1)
    .strftime("%Y-%m-%d")
)

# We need Moving Average of >= 200 days
assert (now - (now - relativedelta(months=args.window))).days >= 200

logging.info(
    "[*] Beginning analysis on range %s - %s", BEG_DT, now.strftime("%Y-%m-%d")
)

# Cache ctl
ON_MISS = {"func": download, "args": [], "kwargs": {"start": BEG_DT}}
ON_HIT = {"func": read_csv, "args": [], "kwargs": {}}
cc = CacheController(
    "cache",
    {
        DataFrame: {
            "name": "to_csv",
            "args": [],
            "kwargs": {"sub": CLOSE_HEADER},
        }
    },
)

logging.info("[*] Stage 1 - Fetching tickers ...")

# ---------------- Stage 1 ----------------
# Get the symbols in the S&P500 with offset
df = read_html(SANDP_500_LINK)[SANDP_500_TABLE_OFFSET]
tickers = df[SANDP_500_TABLE_SYMBOL_HEADER].to_list()

# Periods are for suckers, yfinance needs dashes
tickers = [t.replace(".", "-") for t in tickers]

# Update cache controller `on_miss` to include these in download kwargs
ON_MISS["kwargs"].update({"tickers": tickers})

# Generate a new key for this, anytime tickers or BEG_DT changes
# Key is just all the symbols concatenated + BEG_DT
key = "".join(tickers) + BEG_DT

logging.info("[*] Stage 2 - Fetching data for tickers ...")


# ---------------- Stage 2 ----------------
# Fetch data. This either hits or fills cache for next run
df = cc.fetch(key, on_miss=ON_MISS, on_hit=ON_HIT)
# Drop any NaN data
df = df.dropna(axis=1)

# Special case: if cache is filled this run, date is already index. If
# cache is hit this run, date needs to be told it's the index now
if "Date" in df:
    df["Date"] = to_datetime(df["Date"])
    df = df.set_index("Date")
df.index = to_datetime(df.index)

logging.info("[*] Stage 3 - Performing RSI calculations ...")

last_trading_day = datetime.now() - BDay(1)

# ---------------- Stage 3 ----------------
buys, sells = [], []
for ticker in df:
    ndf = df[[ticker]].copy()
    ndf["MA200"] = ndf[ticker].rolling(window=200).mean()
    ndf["Delta"] = ndf[ticker].pct_change()
    ndf["Upmove"] = ndf["Delta"].apply(lambda x: x if x > 0 else 0)
    ndf["Downmove"] = ndf["Delta"].apply(lambda x: abs(x) if x < 0 else 0)
    ndf["μUp"] = ndf["Upmove"].ewm(alpha=1/args.period).mean()
    ndf["μDown"] = ndf["Downmove"].ewm(alpha=1/args.period).mean()
    ndf.dropna(inplace=True)
    ndf["RS"] = ndf["μUp"] / ndf["μDown"]
    ndf["RSI"] = ndf["RS"].apply(lambda x: 100 - (100 / (x + 1)))
    ndf.loc[(ndf[ticker] > ndf["MA200"]) & (ndf["RSI"] < 30), "Buy"] = "Yes"
    ndf.loc[(ndf[ticker] < ndf["MA200"]) | (ndf["RSI"] > 30), "Buy"] = "No"

    yesdf = ndf.loc[ndf["Buy"] == "Yes"]
    for i in range(len(yesdf)):
        obj = yesdf.iloc[i]
        if last_trading_day < obj.name < datetime.now():
            buys.append(obj)

logging.info(
    "[*] ---------- Generating Candidates from last trading day %s ----------",
    last_trading_day.strftime("%Y-%m-%d")
)

if len(buys) == 0:
    logging.info("[*] Woops, no candidates could be found")

for series in buys + sells:
    symbol = series.index[0]
    rsi = series["RSI"]
    buy = series["Buy"]
    logging.info(f"[*] {symbol:5}: RSI={rsi:.3f} Buy={buy}")
