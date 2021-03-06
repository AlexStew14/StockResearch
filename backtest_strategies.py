import numpy as np
from ta.momentum import RSIIndicator
from ta.trend import SMAIndicator


def rsi_strategy(stock_df, **kwargs):    
    params = BACKTEST_STRATEGY_PARAMS[rsi_strategy]
    if not all(param in kwargs for param in params):
        return None        
    
    long_signals, short_signals = np.zeros(stock_df.shape[0]), np.zeros(stock_df.shape[0])
    rsi_series = RSIIndicator(stock_df['close']).rsi()
    long_signals[rsi_series < kwargs['buy_level']] = 1
    short_signals[rsi_series > kwargs['short_level']] = 1
    return long_signals, short_signals


def baseline_strategy(stock_df, **kwargs):
    long_signals, short_signals = np.ones(stock_df.shape[0]), np.ones(stock_df.shape[0])
    return long_signals, short_signals

def ma_crossover_strategy(stock_df, **kwargs):
    params = BACKTEST_STRATEGY_PARAMS[ma_crossover_strategy]
    if not all(param in kwargs for param in params):
        return None    
    
    long_signals, short_signals = np.zeros(stock_df.shape[0]), np.zeros(stock_df.shape[0])

    fast_ma_series = SMAIndicator(stock_df['close'], kwargs['fast_ma']).sma_indicator()
    slow_ma_series = SMAIndicator(stock_df['close'], kwargs['slow_ma']).sma_indicator()
    long_signals[(fast_ma_series > slow_ma_series) & (fast_ma_series.shift(1) < slow_ma_series.shift(1))] = 1
    short_signals[(fast_ma_series < slow_ma_series) & (fast_ma_series.shift(1) > slow_ma_series.shift(1))] = 1    
    return long_signals, short_signals


BACKTEST_STRATEGY_PARAMS = {
    rsi_strategy: ['buy_level', 'short_level'],
    baseline_strategy: [],
    ma_crossover_strategy: ['fast_ma', 'slow_ma']
}



if __name__ == "__main__":
    import pandas as pd
    args = {'fast_ma': 50, 'slow_ma':200}
    path = 'data/daily/TSLA_2022-04-24.csv'
    stock_df = pd.read_csv(path)
    signals = ma_crossover_strategy(stock_df, **args)    