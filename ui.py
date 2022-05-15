from tkinter import *
from tkinter.ttk import *
from fmpcloud_interface import download_daily_data
import pandas as pd
from backtest import Backtester, BacktestPipeline
from backtest_strategies import *
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

VALID_TICKERS = set(pd.read_csv("data/tickers/all_tradable_symbols.csv", encoding='latin1')['symbol'].tolist())

class Main_UI():

    def __init__(self):
        self.backtester = Backtester(periods=[1, 5, 10, 20, 30], using_tkinter=True)

        self.root = Tk()
        self.root.protocol("WM_DELETE_WINDOW", self.root.quit)
        self.root.title("Backtest")
        self.root.geometry("1280x720")

        self.status_text = Label(self.root, text="", relief=SUNKEN, anchor=W)
        self.status_text.pack(side=BOTTOM, fill=X)

        ticker_label = Label(text="Enter Ticker")
        ticker_label.pack()

        ticker_string_var = StringVar()
        ticker_string_var.trace("w", lambda name, index, mode, sv=ticker_string_var: self.ticker_entered_event(sv))
        ticker_entry = Entry(textvariable=ticker_string_var)        
        ticker_entry.pack()
        self.selected_ticker = ""

        self.supported_strategies_dict = {"RSI Long/Short": rsi_strategy, "MA CrossOver": ma_crossover_strategy, "MACD": None}
        self.selected_strategy_string = StringVar(self.root)        
        self.strategy_option_menu = OptionMenu(self.root, self.selected_strategy_string, "Select Strategy", *list(self.supported_strategies_dict.keys()), command=self.strategy_selected_event)        
        self.strategy_option_menu.pack()     

        self.strategy_params_canvas = Canvas(self.root, bg="gray")        
        self.strategy_params_canvas.pack()

        self.supported_plots_dict = {'Horizontal Plot': self.backtester.plot_results, 'Detailed Plot': self.backtester.plot_detailed_results, 'Timed Plot': self.backtester.plot_timed_results}
        self.selected_plot_string = StringVar(self.root)
        self.plot_selection_menu = OptionMenu(self.root, self.selected_plot_string, "Select Plot", *list(self.supported_plots_dict.keys()), command=self.plot_selected_event)
        self.plot_selection_menu.pack()
        self.plot_method = None

        self.selected_strategy_params_dict = {}

        self.submit_strategy_button = Button(self.root, text="Submit Strategy", command=self.submit_strategy_event)
        self.submit_strategy_button.pack()

        self.plot_canvas = Canvas(self.root, width=1280, height=720)
        self.plot_canvas.pack(side=TOP, fill=BOTH, expand=1)        
        self.main_figure = None

        ticker_entry.focus()

        self.root.mainloop()   


    def ticker_entered_event(self, ticker_string_var):
        self.selected_ticker = ticker_string_var.get().upper()       

    
    def strategy_selected_event(self, *args):
        strategy_string = args[0]        
        strategy_method = self.supported_strategies_dict[strategy_string]
        if strategy_method is None:
            self.status_text.config(text = "UNSUPPORTED STRATEGY")
            return None

        if strategy_method not in BACKTEST_STRATEGY_PARAMS:
            self.status_text.config(text = "STRATEGY NOT IMPLEMENTED")
            return None     

        self.update_strategy_params_canvas()


    def plot_selected_event(self, *args):
        plot_string = args[0]
        self.plot_method = self.supported_plots_dict[plot_string]        
        self.submit_strategy_event()
        

    def update_strategy_params_canvas(self):
        for child in self.strategy_params_canvas.winfo_children():
            child.destroy()

        self.selected_strategy_params_dict = {}
        selected_strategy = self.selected_strategy_string.get()

        for param in BACKTEST_STRATEGY_PARAMS[self.supported_strategies_dict[selected_strategy]]:
            param_label = Label(self.strategy_params_canvas, text=param)
            param_label.pack()
            self.selected_strategy_params_dict[param] = None
            param_entry = Entry(self.strategy_params_canvas)
            param_entry.pack()


    def submit_strategy_event(self):        
        if self.selected_ticker not in VALID_TICKERS:
            self.status_text.config(text = "INVALID TICKER")
            return None

        selected_strategy = self.selected_strategy_string.get()
        if selected_strategy not in self.supported_strategies_dict:
            self.status_text.config(text = "INVALID STRATEGY")
            return None

        param_entries = [child for child in self.strategy_params_canvas.winfo_children() if child.winfo_class() == "TEntry"]
        for param_name, child in zip(self.selected_strategy_params_dict.keys(), param_entries):            
            if not child.get().isdigit():
                self.status_text.config(text = f"INVALID PARAMETER: {param_name}")
                return None
            self.selected_strategy_params_dict[param_name] = int(child.get())
                
        pipeline = BacktestPipeline(**{'method': self.supported_strategies_dict[selected_strategy], 'params': self.selected_strategy_params_dict})

        if self.plot_method is None:
            self.status_text.config(text = "NO PLOT SELECTED")
            return None

        figure = self.plot_method(self.selected_ticker, pipeline)

        if self.main_figure is not None:
            self.plot_canvas.winfo_children()[0].destroy()

        self.main_figure = FigureCanvasTkAgg(figure, master=self.plot_canvas)
        self.main_figure.draw()
        self.main_figure.get_tk_widget().pack(side=TOP, fill=BOTH, expand=1)

        self.status_text.config(text = "STRATEGY SUBMITTED")

        
if __name__ == "__main__":
    main_ui = Main_UI()    