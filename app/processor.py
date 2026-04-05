"""Data processing utilities for stock OHLCV DataFrames."""

import pandas as pd


class DataProcessor:
    """Cleans raw DataFrames and computes all derived metrics."""

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize and clean a raw OHLCV DataFrame.

        Preconditions:
          - df has columns: Open, High, Low, Close, Volume
          - df.index is DatetimeIndex or convertible to one
        Postconditions:
          - No NaN values remain in output
          - Index is DatetimeIndex normalized to UTC date
          - All OHLCV columns are float/int typed
          - high >= low for every row
        """
        # Normalize index to UTC date
        df = df.copy()
        df.index = pd.to_datetime(df.index, utc=True).normalize()
        df.index.name = "date"

        # Rename columns to lowercase
        df.columns = [c.lower() for c in df.columns]

        # Forward-fill missing values
        df = df.ffill()

        # Drop any remaining NaN rows
        df = df.dropna()

        # Enforce types
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].astype(float)
        df["volume"] = df["volume"].astype(int)

        # Remove rows where high < low (data corruption)
        df = df[df["high"] >= df["low"]]

        return df

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Compute derived metrics on a cleaned OHLCV DataFrame.

        Preconditions:
          - df is cleaned (no NaNs, correct types)
          - df has columns: open, high, low, close, volume
        Postconditions:
          - df has additional columns: daily_return, ma_7, volatility
          - Rows with insufficient rolling history are dropped
        """
        df = df.copy().sort_index()

        # Daily return: (close - open) / open
        df["daily_return"] = (df["close"] - df["open"]) / df["open"]

        # 7-day moving average of closing price
        df["ma_7"] = df["close"].rolling(window=7, min_periods=7).mean()

        # Volatility: rolling 7-day std dev of daily returns
        df["volatility"] = df["daily_return"].rolling(window=7, min_periods=7).std()

        # Drop rows where rolling metrics are NaN (first 6 rows)
        df = df.dropna(subset=["ma_7", "volatility"])

        return df

    def compute_daily_return(self, df: pd.DataFrame) -> pd.Series:
        """Return (close - open) / open for each row."""
        return (df["close"] - df["open"]) / df["open"]

    def compute_moving_average(self, df: pd.DataFrame, window: int) -> pd.Series:
        """Return rolling mean of close with the given window."""
        return df["close"].rolling(window=window, min_periods=window).mean()

    def compute_52w_high_low(self, df: pd.DataFrame) -> tuple[float, float]:
        """Return (max high, min low) over the last 52 weeks."""
        return compute_52w_high_low(df)

    def compute_volatility_score(self, df: pd.DataFrame) -> pd.Series:
        """Return rolling 7-day std dev of daily returns."""
        daily_return = self.compute_daily_return(df)
        return daily_return.rolling(window=7, min_periods=7).std()


def compute_52w_high_low(df: pd.DataFrame) -> tuple[float, float]:
    """
    Return (max high, min low) over the last 52 weeks of data.

    Preconditions:
      - df has columns 'high' and 'low' with float values
      - df contains at least 1 row
    Postconditions:
      - Returns (max_high, min_low) over the last 52 weeks
    """
    cutoff = df.index.max() - pd.DateOffset(weeks=52)
    window = df[df.index >= cutoff]
    return float(window["high"].max()), float(window["low"].min())
