import plotly.graph_objects as go


def plot_candlesticks(df, width=1200, height=600):
    fig = go.Figure(data=[go.Candlestick(x=df['date'],
                open=df['open'],
                high=df['high'],
                low=df['low'],
                close=df['close'])])
    fig.update_layout(xaxis_rangeslider_visible=False, width=width, height=height, margin={'l': 10, 'r': 10, 'b': 10, 't': 10})
    fig.show()