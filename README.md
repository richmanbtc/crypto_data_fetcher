crypto_data_fetcher

## Features

- fetch (ohlcv/funding rate) as pandas DataFrame
- incremental fetch

## Install

```bash
pip install "git+https://github.com/richmanbtc/crypto_data_fetcher.git@v0.0.4#egg=crypto_data_fetcher"
```

## Usage



## Supported exchange and data

|exchange|market_type|ohlcv|mark ohlcv|funding rate|premium index|
|:-:|:-:|:-:|:-:|:-:|:-:|
|binance|spot|o|o|o|x|
|binance|future|o|o|o|x|
|binance|derivative|o|o|o|x|
|bybit|inverse|o|o|o|o|
|bybit|usdt|o|o|o|o|
|ftx|all|o|o|o|x|

## License

CC0

## Developer

### test

```bash
python3 -m unittest tests/test_*
```

```bash
pyenv exec pipenv run python3 -m unittest tests/test_*
```
