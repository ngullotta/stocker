import logging
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime

from dateutil.relativedelta import relativedelta
from numpy import prod
from pandas import DataFrame, read_csv, read_html, to_datetime
from yfinance import Ticker, download

from stocker.cache import CacheController

parser = ArgumentParser()
config = ConfigParser()
parser.add_argument(
    "-w",
    "--window",
    type=int,
    default=12,
    help="Historical time period in months to go back for financial data",
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
for section in ["s&p-500-momentum", "yfinance"]:
    failed = False
    if section not in config.sections():
        failed = True
        logging.error(f'[!] "{section}" not found in config')
        logging.error("[!] Please copy original config or fix content")
    if failed:
        exit(1)

# ---------------- Constants ----------------
now = datetime.now()
sconfig = config["s&p-500-momentum"]
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

# ---------------- Stage 1 ----------------
# Get the symbols in the NASDAQ top 100 with offset
df = read_html(SANDP_500_LINK)[SANDP_500_TABLE_OFFSET]
tickers = df[SANDP_500_TABLE_SYMBOL_HEADER].to_list()

# Update cache controller `on_miss` to include these in download kwargs
ON_MISS["kwargs"].update({"tickers": tickers})

# Generate a new key for this, anytime tickers or BEG_DT changes
# Key is just all the symbols concatenated + BEG_DT
key = "".join(tickers) + BEG_DT

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

# Calculate percent change and resample this to monthly percent change
# Skips the first row (will be NaN's)
mtl = (df.pct_change() + 1)[1:].resample("M").prod()

# ---------------- Stage 3 ----------------
# Grab rolling window of 12 month, 6 month, and 3 month percent changes
# and use each to screen the next X entries
indices = None
for months, number in [(12, 100), (6, 50), (3, 10)]:
    df = mtl.rolling(months).apply(prod)
    if indices is not None:
        top = df.loc[df.index[-1], indices].nlargest(number)
    else:
        top = df.loc[df.index[-1]].nlargest(number)
    indices = top.index

# The big reveal!
for symbol in indices:
    logging.info("[*] %s", symbol)
    ticker = Ticker(symbol)
    logging.info("[^] <%s>", ticker.recommendations[-1:]["To Grade"].values[0])
    print()