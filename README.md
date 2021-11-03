# StockResearch

This repository is a coherent library of my research into systematic trading.
Historical stock data is stored in csv files (ignored) and then loaded onto a local mysql database for factor research and visualization.

The general workflow is:
    1. Define a universe of stocks as a subset of all stocks based on mask criteria.
    2. Define a factor that has the potential of having predictive power.
    3. Split universe into quantiles based on factor and record price change for each quantile over multiple timeframes.
    4. Once multiple predictive factors are found they can be combined using machine learning (RandomForest) or some other method.
    5. Combined factors can then be used to create a long/short system for algorithmic trading.