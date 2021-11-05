def initialize_daily_table(engine):
    engine.execute("CREATE TABLE `daily` (\n\
    `date` datetime NOT NULL,\n\
    `open` double NOT NULL,\n\
    `high` double NOT NULL,\n\
    `low` double NOT NULL,\n\
    `close` double NOT NULL,\n\
    `adjClose` double DEFAULT NULL,\n\
    `volume` double DEFAULT NULL,\n\
    `unadjustedVolume` double DEFAULT NULL,\n\
    `change` double DEFAULT NULL,\n\
    `changePercent` double DEFAULT NULL,\n\
    `vwap` double DEFAULT NULL,\n\
    `ticker` text,\n\
    UNIQUE KEY `date_ticker_index` (`date`,`ticker`(20))\n\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")


def drop_table(engine, inspector, table_name):
    if inspector.has_table(table_name):
        engine.execute(f"DROP TABLE {table_name};")

def read_csvs_into_daily(engine, inspector):
    data_dir = os.getcwd()+'\\data'

    if not inspector.has_table('daily'):
        initialize_daily_table(engine)    

    aggregate_df = None
    for i, filename in enumerate(os.listdir(data_dir)):
        if filename[-3:] != 'csv':
            continue

        file_path = os.path.join(data_dir, filename)
        
        df = pd.read_csv(file_path)
        df.date = df.date.astype('datetime64[D]')
        df.drop(columns=['changeOverTime', 'label'], inplace=True)
        df['ticker'] = filename.split('_')[0].upper()
        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.dropna()

        if aggregate_df is None:
            aggregate_df = df.copy()
        else:
            aggregate_df = aggregate_df.append(df)
        
        if df.shape[0] < 10:
            continue

        if i % 100 == 0:
            aggregate_df.to_sql('temp_table', con=engine, if_exists='replace', index=False)
            aggregate_df = None
            print(str(i))
            engine.execute('INSERT IGNORE INTO daily SELECT * FROM temp_table;')
            engine.execute("DROP TABLE temp_table;")

    aggregate_df.to_sql('temp_table', con=engine, if_exists='replace', index=False)
    print(str(i))
    engine.execute('INSERT IGNORE INTO daily SELECT * FROM temp_table;')
    engine.execute("DROP TABLE temp_table;")

    
def initialize_tickers_table(engine):
    engine.execute("CREATE TABLE `tickers` (\n\
    `ticker` VARCHAR(20) PRIMARY KEY,\n\
    `exchange` text NOT NULL\n\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")


def read_csv_into_tickers(engine, inspector):
    if not inspector.has_table('tickers'):
        initialize_tickers_table(engine)  

    data_dir = os.getcwd() +'\\data\\tickers'

    for filename in os.listdir(data_dir):
        if filename[-3:] != 'csv':
            continue
        
        file_path = os.path.join(data_dir, filename)        
        rawdata = open(file_path, 'rb').read()
        encoding = chardet.detect(rawdata)['encoding']
        df = pd.read_csv(file_path, encoding=encoding)
        df = df.drop(columns=['name', 'price', 'exchange'])
        df = df.rename(columns={'symbol': 'ticker', 'exchangeShortName': 'exchange'})
        df = df[df['exchange'].isin(['NYSE', 'NASDAQ', 'ETF'])]

        df.to_sql('temp_ticker_table', con=engine, if_exists='replace', index=False)
        engine.execute('INSERT IGNORE INTO tickers SELECT * FROM temp_ticker_table;')
        engine.execute("DROP TABLE temp_ticker_table;")


def download_all_ticker_csv_files(engine, inspector):
    if inspector.has_table('tickers'):
        tickers = engine.execute("SELECT ticker from tickers").fetchall()
        tickers = [t[0] for t in tickers]

        for ticker in tickers:
            fmp.pull_daily_change_and_volume_csv(ticker)


if __name__ == '__main__':
    import pandas as pd
    import sqlalchemy as sqla
    import numpy as np
    import os
    import sys
    import chardet
    import fmpcloud_interface as fmp    

    engine = sqla.create_engine('mysql+pymysql://user:user@localhost:3306/stock_data', echo=False)    
    inspector = sqla.inspect(engine)

    if len(sys.argv) > 1:
        if sys.argv[1] == 'init':
            initialize_daily_table(engine)
            print("daily table sucessfully created.")
            exit()
        elif sys.argv[1] == 'read':
            read_csvs_into_daily(engine, inspector)
            print("Read data csv files into the Daily table.")
            exit()
        elif sys.argv[1] == 'reset':
            drop_table(engine, inspector, 'daily')
            print("Dropped daily table.")
            initialize_daily_table(engine)
            print("Created daily table.")
            read_csvs_into_daily(engine, inspector)
            print("Read data csv files into the Daily table.")            
            exit()
        elif sys.argv[1] == 'drop':
            drop_table(engine, inspector, 'daily')
            print("Dropped daily table.")
            exit()
        else:
            print("Unknown argument!")
            exit()

    #download_all_ticker_csv_files(engine, inspector)
    #drop_table(engine, inspector, 'daily')
    # drop_table(engine, inspector, 'tickers')
    read_csvs_into_daily(engine, inspector)
        
    