from sqlalchemy.sql.expression import table


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


def initialize_tickers_table(engine):
    engine.execute("CREATE TABLE `tickers` (\n\
    `ticker` VARCHAR(20) PRIMARY KEY,\n\
    `exchange` text NOT NULL\n\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;")


def drop_table(engine, inspector, table_name):
    if inspector.has_table(table_name):
        engine.execute(f"DROP TABLE {table_name};")

# def read_csvs_into_daily(engine, inspector):
#     data_dir = os.getcwd()+'\\data'

#     if not inspector.has_table('daily'):
#         initialize_daily_table(engine)    

#     aggregate_df = None
#     for i, filename in enumerate(os.listdir(data_dir)):
#         if filename[-3:] != 'csv':
#             continue

#         file_path = os.path.join(data_dir, filename)
        
#         df = pd.read_csv(file_path)
#         df.date = df.date.astype('datetime64[D]')
#         df.drop(columns=['changeOverTime', 'label'], inplace=True)
#         df['ticker'] = filename.split('_')[0].upper()
#         df.replace([np.inf, -np.inf], np.nan, inplace=True)
#         df = df.dropna()

#         if aggregate_df is None:
#             aggregate_df = df.copy()
#         else:
#             aggregate_df = aggregate_df.append(df)
        
#         if df.shape[0] < 10:
#             continue

#         if i % 100 == 0:
#             aggregate_df.to_sql('temp_table', con=engine, if_exists='replace', index=False)
#             aggregate_df = None
#             print(str(i))
#             engine.execute('INSERT IGNORE INTO daily SELECT * FROM temp_table;')
#             engine.execute("DROP TABLE temp_table;")

#     aggregate_df.to_sql('temp_table', con=engine, if_exists='replace', index=False)
#     print(str(i))
#     engine.execute('INSERT IGNORE INTO daily SELECT * FROM temp_table;')
#     engine.execute("DROP TABLE temp_table;")

    

# def read_csv_into_tickers(engine, inspector):
#     if not inspector.has_table('tickers'):
#         initialize_tickers_table(engine)  

#     data_dir = os.getcwd() +'\\data\\tickers'

#     for filename in os.listdir(data_dir):
#         if filename[-3:] != 'csv':
#             continue
        
#         file_path = os.path.join(data_dir, filename)        
#         rawdata = open(file_path, 'rb').read()
#         encoding = chardet.detect(rawdata)['encoding']
#         df = pd.read_csv(file_path, encoding=encoding)
#         df = df.drop(columns=['name', 'price', 'exchange'])
#         df = df.rename(columns={'symbol': 'ticker', 'exchangeShortName': 'exchange'})
#         df = df[df['exchange'].isin(['NYSE', 'NASDAQ', 'ETF'])]

#         df.to_sql('temp_ticker_table', con=engine, if_exists='replace', index=False)
#         engine.execute('INSERT IGNORE INTO tickers SELECT * FROM temp_ticker_table;')
#         engine.execute("DROP TABLE temp_ticker_table;")

def __read_csvs(csv_list, table_name, process_id):
    import sqlalchemy as sqla
    import pandas as pd
    import os

    engine = sqla.create_engine('mysql+pymysql://user:user@localhost:3306/stock_data', echo=False)    
    inspector = sqla.inspect(engine)
    # if not inspector.has_table(table_name):
    #     return

    # ADD PREPROCESSING BASED ON TABLE NAME
    # CAN SET FUNCTION SO YOU DONT HAVE TO DO IFS DURING LOOP
    
    temp_table = f'temp_table_{process_id}'
    for csv in csv_list:
        df = pd.read_csv(csv)
        file_name = os.path.split(csv_list[0])[-1]
        ticker = file_name.split('_')[0]        
        df['ticker'] = ticker
        df.to_sql(temp_table, con=engine, index=False, if_exists='replace')
        engine.execute(f"INSERT IGNORE INTO {table_name} SELECT * FROM {temp_table};")
        engine.execute(f"DROP TABLE {temp_table};")


def read_csvs_into_table(csv_dir, table_name, num_processes=1):
    data_dir = os.path.join(os.getcwd(), os.path.abspath(csv_dir))
    
    csv_file_names = [os.path.join(data_dir, name) for name in os.listdir(data_dir) if name[-4:] == '.csv']
    if num_processes > 1:
        csv_per_process = len(csv_file_names) // num_processes
        for i in range(num_processes):
            csv_batch = csv_file_names[i * csv_per_process: (i+1) * csv_per_process]
            process = mp.Process(target=__read_csvs, args=(csv_batch, table_name, i))
            process.start()

    else:
        __read_csvs(csv_file_names, table_name, 0)


def download_all_stock_csvs(engine, inspector, csv_type='ratios'):
    if inspector.has_table('tickers'):
        tickers = engine.execute("SELECT ticker from tickers").fetchall()
        tickers = [t[0] for t in tickers]

        data_interface = fmp.FMPCloudInterface(tickers)
        data_interface.pull_data(csv_type)     


if __name__ == '__main__':
    import pandas as pd
    import sqlalchemy as sqla
    import numpy as np
    import os
    import sys
    import chardet
    import fmpcloud_interface as fmp    
    import multiprocessing as mp

    # engine = sqla.create_engine('mysql+pymysql://user:user@localhost:3306/stock_data', echo=False)    
    # df = pd.read_csv('./data/ratios/A_quarterly_ratios_06_11_21.csv')
    # df['ticker'] = 'A'
    # print(df.info())
    # df.to_sql(name='ratios',con=engine, index=False)

    read_csvs_into_table('data/ratios', 'ratios', 10)


 
    