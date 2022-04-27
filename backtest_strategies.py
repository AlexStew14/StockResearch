import numpy as np
from ta.momentum import RSIIndicator


def rsi_strategy(stock_df, **kwargs):
    # Return if buy_level and short_level not in kwargs
    if 'buy_level' not in kwargs or 'short_level' not in kwargs:
        return None
    
    signals = np.zeros(stock_df.shape[0])
    rsi_series = RSIIndicator(stock_df['close']).rsi()
    signals[rsi_series < kwargs['buy_level']] = 1
    signals[rsi_series > kwargs['short_level']] = -1
    return signals


# Chooses buy and short indices randomly to be used for comparison
def baseline_strategy(stock_df, **kwargs):
    if "sample_count" not in kwargs:
        return None

    signals = np.zeros(stock_df.shape[0])
    buy_indices = np.random.choice(stock_df.shape[0], kwargs["sample_count"], replace=False)
    short_indices = np.random.choice(stock_df.shape[0], kwargs["sample_count"], replace=False)
    signals[buy_indices] = 1
    signals[short_indices] = -1
    return signals