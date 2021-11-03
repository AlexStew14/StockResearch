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


def drop_daily_table(engine, inspector):
    if inspector.has_table('daily'):
        engine.execute("DROP TABLE daily;")

def read_csvs_into_daily(engine, inspector):
    data_dir = os.getcwd()+'\\data'

    for filename in os.listdir(data_dir):
        if filename[-3:] != 'csv':
            continue

        file_path = os.path.join(data_dir, filename)
        df = pd.read_csv(file_path)
        df.date = df.date.astype('datetime64[D]')
        df.drop(columns=['changeOverTime', 'label'], inplace=True)
        df['ticker'] = filename.split('_')[0].upper()

        small_types = {'open': np.float32, 'high': np.float32, 'low': np.float32, 'close': np.float32, 'adjClose': np.float32,
                        'volume': np.int32, 'unadjustedVolume': np.int32, 'change': np.float32, 'changePercent': np.float32,
                        'vwap': np.float32}

        df = df.astype(small_types)

        if not inspector.has_table('daily'):
            initialize_daily_table(engine)    

        df.to_sql('temp_table', con=engine, if_exists='replace', index=False)
        engine.execute('INSERT IGNORE INTO daily SELECT * FROM temp_table')

    engine.execute("DROP TABLE temp_table;")


if __name__ == '__main__':
    import pandas as pd
    import sqlalchemy as sqla
    import numpy as np
    import os
    import sys

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
            drop_daily_table(engine, inspector)
            print("Dropped daily table.")
            initialize_daily_table(engine)
            print("Created daily table.")
            read_csvs_into_daily(engine, inspector)
            print("Read data csv files into the Daily table.")            
            exit()
        elif sys.argv[1] == 'drop':
            drop_daily_table(engine, inspector)
            print("Dropped daily table.")
            exit()
        else:
            print("Unknown argument!")
            exit()
    else:
        drop_daily_table(engine, inspector)
        initialize_daily_table(engine)
        read_csvs_into_daily(engine, inspector)

    