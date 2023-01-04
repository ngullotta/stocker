# Stocker

A collection of stock related analysis scripts using pandas and yfinance

## Intallation

[Poetry](https://python-poetry.org/) is the dependency manager of choice for
this project. Install using the instructions on their website then do:

```shell
$ git clone ...
$ cd stocker
$ poetry install --no-dev
```

## Usage

Just run one of the strategy scripts provided in the root dir. Check out their
--help for optional parameters

```shell
$ poetry run python3 nasdaq_100_momentum.py
```

### Scripts

These are heavily borrowed from the tutorials on
[@Algovibes](https://www.youtube.com/@Algovibes) youtube channel. Some
alterations are made to suit my personal needs.

- `nasdaq_100_momentum`
  - [Borrowed heavily from here](https://www.youtube.com/watch?v=bUejGzheCac)
  - Trade frequency: Monthly
  - Select the 50 best stocks from the NASDAQ 100 over the last 12 months
  - Out of those select the best 30 stocks by percent change over the last 6
    months
  - Out of those select the best 10 stocks by percent change over the last 3
    months
  - Try to eliminate survivorship bias

- `s&p_500_momentum`
  - Same thing as the NASDAQ script but with S&P 500
  - Window moves from 100/50/10

- `s&p_500_rsi`
  - Generates RSI indicators for the S&P 500 based on MA of 200 days and an RSI
    period of 2 weeks (10 trading days)
  - Generates candidates that have an RSI < 30 in the last trading day

## ToDo:

- Trading calendar support
- Improve caching
- Implement database support
- Move repeated functionality into core stocker library
- Add crypto analyzer
