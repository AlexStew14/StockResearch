from tkinter import *
from tkinter.ttk import *
from fmpcloud_interface import download_daily_data
import pandas as pd
from backtest import backtest
from backtest_strategies import *
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk


VALID_TICKERS = set(pd.read_csv("data/tickers/all_tradable_symbols.csv", encoding='latin1')['symbol'].tolist())

class Main_UI():

    def __init__(self):
        self.root = Tk()
        self.root.protocol("WM_DELETE_WINDOW", self.root.quit)
        self.root.title("Backtest")
        self.root.geometry("1280x720")

        ticker_label = Label(text="Enter Ticker")
        ticker_label.pack()

        ticker_text = StringVar()
        ticker_entry = Entry(textvariable=ticker_text)
        ticker_entry.bind('<Return>', self.ticker_entered_event)
        ticker_entry.pack()

        self.plot_canvas = Canvas(self.root, width=1280, height=720)
        self.plot_canvas.pack(side=TOP, fill=BOTH, expand=1)        
        self.main_figure = None

        ticker_entry.focus()

        self.root.mainloop()   

    def ticker_entered_event(self, event):
        ticker = event.widget.get().upper()  
        if ticker not in VALID_TICKERS:
            print("INVALID TICKER")
            return None
            
        stock_df = download_daily_data([ticker], write_to_file=False)[0]
        sample_pipeline = {'method': rsi_strategy, 'params': {'buy_level': 35, 'short_level': 80}}
        figure = backtest(ticker, stock_df, sample_pipeline, periods=[1,5,10,30], baseline_compare=True, using_tkinter=True)

        if self.main_figure is not None:
            self.plot_canvas.winfo_children()[0].destroy()

        self.main_figure = FigureCanvasTkAgg(figure, master=self.plot_canvas)
        self.main_figure.draw()
        self.main_figure.get_tk_widget().pack(side=TOP, fill=BOTH, expand=1)

if __name__ == "__main__":
    main_ui = Main_UI()    