import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from backtest_strategies import *


def backtest_stratgey(stock_df, pipeline, periods):
    signals = pipeline['method'](stock_df, **pipeline['params'])
    trade_results = {}    

    trade_opens_long = stock_df.iloc[signals == 1]['close'].values
    trade_opens_short = stock_df.iloc[signals == -1]['close'].values

    for period in periods:
        # Get indicies of buy signals in signals array
        buy_indices = np.clip(np.where(signals == 1)[0] + period, 0, signals.shape[0]-1)
        trade_closes_long = stock_df.iloc[buy_indices]['close'].values        
        # Get indicies of short signals in signals array
        short_indices = np.clip(np.where(signals == -1)[0] + period, 0, signals.shape[0]-1)
        trade_closes_short = stock_df.iloc[short_indices]['close'].values

        trade_results[f"{period}_day_long"] = np.array((trade_closes_long - trade_opens_long) / trade_opens_long * 100)
        trade_results[f"{period}_day_short"] = np.array((trade_opens_short - trade_closes_short) / trade_opens_short * 100)
    

    labels = [f"{period} day" for period in periods]
    all_long_results_df = pd.DataFrame(np.array([trade_results[f"{period}_day_long"] for period in periods]).T, columns=labels)
    all_short_results_df = pd.DataFrame(np.array([trade_results[f"{period}_day_short"] for period in periods]).T, columns=labels)
    return all_long_results_df,all_short_results_df


def plot_backtest(ticker, stock_df, pipeline, periods, baseline_compare, all_long_results_df, all_short_results_df, using_tkinter=False):
    if baseline_compare:
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)    
    else:
        fig, (ax1, ax2) = plt.subplots(1, 2)
        

    sns.stripplot(data=all_long_results_df, ax=ax1, color='.25', jitter=True)
    sns.boxplot(data=all_long_results_df, ax=ax1)
    ax1.title.set_text("Long")
    ax1.set_ylabel("% Return")

    sns.stripplot(data=all_short_results_df, ax=ax2, color='.25', jitter=True)    
    sns.boxplot(data=all_short_results_df, ax=ax2)
    ax2.title.set_text("Short")    
    ax2.set_ylabel("% Return")    

    if baseline_compare:
        all_long_results_df, all_short_results_df = backtest_stratgey(stock_df, {'method': baseline_strategy, 'params': {'sample_count': 100}}, periods)
        sns.stripplot(data=all_long_results_df, ax=ax3, color='.25', jitter=True)
        sns.boxplot(data=all_long_results_df, ax=ax3)
        ax3.title.set_text("Long")
        ax3.set_ylabel("% Return")

        sns.stripplot(data=all_short_results_df, ax=ax4, color='.25', jitter=True)    
        sns.boxplot(data=all_short_results_df, ax=ax4)
        ax4.title.set_text("Short")    
        ax4.set_ylabel("% Return")

    fig.suptitle(f"Stock: {ticker}\nPipeline: {pipeline['method'].__name__} - {pipeline['params']}\nDate Range: {stock_df.date.iloc[0]} - {stock_df.date.iloc[-1]}")
    if using_tkinter:
        return fig
    plt.show()


def backtest(ticker, stock_df, pipeline, periods=[1,3,5], baseline_compare=True, using_tkinter=False):
    all_long_results_df, all_short_results_df = backtest_stratgey(stock_df, pipeline, periods)    
    return plot_backtest(ticker, stock_df, pipeline, periods, baseline_compare, all_long_results_df, all_short_results_df, using_tkinter=using_tkinter)



if __name__ == '__main__':
    sample_pipeline = {'method': rsi_strategy, 'params': {'buy_level': 35, 'short_level': 80}}
    path = 'data/daily/TSLA_2022-04-24.csv'
    stock_df = pd.read_csv(path)
    backtest(path.split("/")[-1].split("_")[0], stock_df, sample_pipeline, periods=[1,5,10,30], baseline_compare=True)
