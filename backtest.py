import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from collections import OrderedDict

from fmpcloud_interface import download_daily_data
from backtest_strategies import *


class BacktestPipeline:
    def __init__(self, **kwargs):
        if 'method' not in kwargs:
            raise Exception('method is required')
        
        if not all(key in kwargs['params'] for key in BACKTEST_STRATEGY_PARAMS[kwargs['method']]):
            raise Exception('missing required parameters')

        self.kwargs = kwargs        
        self.strategy_name = kwargs['method'].__name__
        self.strategy_params = OrderedDict(sorted(kwargs['params'].items()))        
        self._strategy_params_hashable = tuple(self.strategy_params.items())

    def __hash__(self):
        return hash(self.strategy_name) + hash(self._strategy_params_hashable)

    def __getitem__(self, key):
        return self.kwargs[key]


class Backtester():
    def __init__(self, periods=[1,3,5], 
                baseline_pipeline={'method': baseline_strategy, 'params': {'sample_count': 500}}, 
                using_tkinter=False):

        self.periods = periods
        self.baseline_pipeline = baseline_pipeline
        self.using_tkinter = using_tkinter

        # Dictionary of ticker: stock dataframe
        self.stock_dfs = {}

        # Dictionaries of ticker: BacktestPipeline: long/short/baseline_long/baseline_short: results dataframe
        self.all_results_dict = {}


    def _backtest(self, stock_df, pipeline):
        long_signals, short_signals = pipeline['method'](stock_df, **pipeline['params'])
        long_trades = []
        short_trades = []
        long_labels, short_labels = [], []

        trade_opens_long = stock_df.iloc[long_signals == 1]['close'].values
        trade_opens_short = stock_df.iloc[short_signals == 1]['close'].values

        for period in self.periods:
            # Get indicies of buy signals in signals array
            buy_indices = np.clip(np.where(long_signals == 1)[0] + period, 0, long_signals.shape[0]-1)
            trade_closes_long = stock_df.iloc[buy_indices]['close'].values    
            trade_dates_long = stock_df.iloc[buy_indices]['date'].values    
            # Get indicies of short signals in signals array
            short_indices = np.clip(np.where(short_signals == 1)[0] + period, 0, short_signals.shape[0]-1)
            trade_closes_short = stock_df.iloc[short_indices]['close'].values
            trade_dates_short = stock_df.iloc[short_indices]['date'].values

            long_trades.append(np.array((trade_closes_long - trade_opens_long) / trade_opens_long * 100))
            long_labels.append(f'{period} day')

            short_trades.append(np.array((trade_opens_short - trade_closes_short) / trade_opens_short * 100))
            short_labels.append(f'{period} day')

            long_trades.append(trade_dates_long)
            long_labels.append(f'{period} day dates')

            short_trades.append(trade_dates_short)
            short_labels.append(f'{period} day dates')

        
        all_long_results_df = pd.DataFrame(np.array(long_trades).T, columns=long_labels)
        all_short_results_df = pd.DataFrame(np.array(short_trades).T, columns=short_labels)

        return all_long_results_df, all_short_results_df


    def _run(self, ticker, pipeline):
        self.all_results_dict[ticker][pipeline] = {}
        long, short = self._backtest(self.stock_dfs[ticker], pipeline)            
        self.all_results_dict[ticker][pipeline]['long'] = long
        self.all_results_dict[ticker][pipeline]['short'] = short
            
        baseline_long, baseline_short = self._backtest(self.stock_dfs[ticker], self.baseline_pipeline)
        self.all_results_dict[ticker][pipeline]['baseline_long'] = baseline_long
        self.all_results_dict[ticker][pipeline]['baseline_short'] = baseline_short


    def _check_cache(func):
        def inner(self, ticker, pipeline):
            if ticker not in self.stock_dfs:
                self.stock_dfs[ticker] = download_daily_data([ticker], write_to_file=False)[0]

            if ticker not in self.all_results_dict:
                self.all_results_dict[ticker] = {}

            if pipeline not in self.all_results_dict[ticker]:
                self._run(ticker, pipeline)
            
            return func(self, ticker, pipeline)
        return inner


    def _plot_distribution_vertical(self, all_results_df, ax, title, ylabel):
        sns.stripplot(data=all_results_df, ax=ax, color='.25', jitter=True)
        sns.boxplot(data=all_results_df, ax=ax)
        ax.title.set_text(title)
        ax.set_ylabel(ylabel)


    def _plot_distribution_horizontal(self, data, ax, title, xlabel, label, color):
        sns.histplot(data=data, ax=ax, color=color, fill=True, stat='density', alpha=.6, common_norm=False, multiple='layer', element='step', label=f"{label}\navg: {data.mean():.2f}%")
        ax.title.set_text(title)
        ax.set_xlabel(xlabel)


    @_check_cache
    def plot_results(self, ticker, pipeline):
        results = self.all_results_dict[ticker][pipeline]

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2)    
            
        self._plot_distribution_vertical(results['long'], ax1, "Long", "% Return")
        self._plot_distribution_vertical(results['short'], ax2, "Short", "% Return")

        self._plot_distribution_vertical(results['baseline_long'], ax3, "Long", "% Return")
        self._plot_distribution_vertical(results['baseline_short'], ax4, "Short", "% Return")

        fig.suptitle(f"Stock: {ticker}\nPipeline: {pipeline['method'].__name__} - {pipeline['params']}\nDate Range: {self.stock_dfs[ticker].date.iloc[0]} - {self.stock_dfs[ticker].date.iloc[-1]}")
        if self.using_tkinter:
            return fig
        plt.show()


    @_check_cache
    def plot_detailed_results(self, ticker, pipeline):
        results = self.all_results_dict[ticker][pipeline]

        fig, (axs1, axs2) = plt.subplots(2, len(self.periods)) 
        
        for period, ax in zip(self.periods, axs1):
            self._plot_distribution_horizontal(results['baseline_long'][f"{period} day"], ax, f"Long Period: {period}", "% Return", "Baseline", color="red")
            self._plot_distribution_horizontal(results['long'][f"{period} day"], ax, f"Long Period: {period}", "% Return", "Strategy", color="blue")                
            ax.legend(loc='upper left')

        for period, ax in zip(self.periods, axs2):
            self._plot_distribution_horizontal(results['baseline_short'][f"{period} day"], ax, f"Short Period: {period}", "% Return", "Baseline", color="red")
            self._plot_distribution_horizontal(results['short'][f"{period} day"], ax, f"Short Period: {period}", "% Return", "Strategy", color="blue")            
            ax.legend()
        
        fig.suptitle(f"Stock: {ticker}\nPipeline: {pipeline['method'].__name__} - {pipeline['params']}\nDate Range: {self.stock_dfs[ticker].date.iloc[0]} - {self.stock_dfs[ticker].date.iloc[-1]}")        
        if self.using_tkinter:
            return fig
        plt.show()

    @_check_cache
    def plot_timed_results(self, ticker, pipeline):
        results = self.all_results_dict[ticker][pipeline]

        fig, (axs1, axs2) = plt.subplots(2, len(self.periods))

        for period, ax in zip(self.periods, axs1):
            sns.scatterplot(x=results['long'][f'{period} day dates'], y=results['long'][f'{period} day'], ax=ax)
            sns.lineplot(x=results['long'][f'{period} day dates'], y=results['long'][f'{period} day'].rolling(window=5).mean(), ax=ax)
            plt.setp(ax.get_xticklabels(), rotation=45, horizontalalignment='right', fontsize=8)

        for period, ax in zip(self.periods, axs2):
            sns.scatterplot(x=results['short'][f'{period} day dates'], y=results['short'][f'{period} day'], ax=ax)
            sns.lineplot(x=results['short'][f'{period} day dates'], y=results['short'][f'{period} day'].rolling(window=5).mean(), ax=ax)
            plt.setp(ax.get_xticklabels(), rotation=45, horizontalalignment='right', fontsize=8)

        fig.suptitle(f"Stock: {ticker}\nPipeline: {pipeline['method'].__name__} - {pipeline['params']}\nDate Range: {self.stock_dfs[ticker].date.iloc[0]} - {self.stock_dfs[ticker].date.iloc[-1]}")        
        if self.using_tkinter:
            return fig
        plt.show()


    

if __name__ == '__main__':
    sample_pipeline = BacktestPipeline(**{'method': rsi_strategy, 'params': {'buy_level': 35, 'short_level': 80}})
    path = 'data/daily/TSLA_2022-04-24.csv'
    ticker = path.split("/")[-1].split("_")[0]
    stock_df = pd.read_csv(path)
    backtest = Backtester(periods=[1,5,10,30])
    #backtest.plot_results("AMD", sample_pipeline)
    #backtest.plot_detailed_results("AMD", sample_pipeline)
    backtest.plot_timed_results("AMD", sample_pipeline)
