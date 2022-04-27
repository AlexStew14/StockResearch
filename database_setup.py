def initialize_daily_table(engine):
    engine.execute(
        "CREATE TABLE `daily` (\n\
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
    INDEX (ticker(20))\n\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
    )


def initialize_tickers_table(engine):
    engine.execute(
        "CREATE TABLE `tickers` (\n\
    `ticker` VARCHAR(20) PRIMARY KEY,\n\
    `exchange` text NOT NULL\n\
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"
    )


def initialize_ratios_table(engine):
    engine.execute(
        """CREATE TABLE `ratios` (
            `date` datetime DEFAULT NULL,
            `period` text,
            `currentRatio` double DEFAULT NULL,
            `quickRatio` double DEFAULT NULL,
            `cashRatio` double DEFAULT NULL,
            `daysOfSalesOutstanding` double DEFAULT NULL,
            `daysOfInventoryOutstanding` double DEFAULT NULL,
            `operatingCycle` double DEFAULT NULL,
            `daysOfPayablesOutstanding` double DEFAULT NULL,
            `cashConversionCycle` double DEFAULT NULL,
            `grossProfitMargin` double DEFAULT NULL,
            `operatingProfitMargin` double DEFAULT NULL,
            `pretaxProfitMargin` double DEFAULT NULL,
            `netProfitMargin` double DEFAULT NULL,
            `effectiveTaxRate` double DEFAULT NULL,
            `returnOnAssets` double DEFAULT NULL,
            `returnOnEquity` double DEFAULT NULL,
            `returnOnCapitalEmployed` double DEFAULT NULL,
            `netIncomePerEBT` double DEFAULT NULL,
            `ebtPerEbit` double DEFAULT NULL,
            `ebitPerRevenue` double DEFAULT NULL,
            `debtRatio` double DEFAULT NULL,
            `debtEquityRatio` double DEFAULT NULL,
            `longTermDebtToCapitalization` double DEFAULT NULL,
            `totalDebtToCapitalization` double DEFAULT NULL,
            `interestCoverage` double DEFAULT NULL,
            `cashFlowToDebtRatio` double DEFAULT NULL,
            `companyEquityMultiplier` double DEFAULT NULL,
            `receivablesTurnover` double DEFAULT NULL,
            `payablesTurnover` double DEFAULT NULL,
            `inventoryTurnover` double DEFAULT NULL,
            `fixedAssetTurnover` double DEFAULT NULL,
            `assetTurnover` double DEFAULT NULL,
            `operatingCashFlowPerShare` double DEFAULT NULL,
            `freeCashFlowPerShare` double DEFAULT NULL,
            `cashPerShare` double DEFAULT NULL,
            `payoutRatio` double DEFAULT NULL,
            `operatingCashFlowSalesRatio` double DEFAULT NULL,
            `freeCashFlowOperatingCashFlowRatio` double DEFAULT NULL,
            `cashFlowCoverageRatios` double DEFAULT NULL,
            `shortTermCoverageRatios` double DEFAULT NULL,
            `capitalExpenditureCoverageRatio` double DEFAULT NULL,
            `dividendPaidAndCapexCoverageRatio` double DEFAULT NULL,
            `dividendPayoutRatio` double DEFAULT NULL,
            `priceBookValueRatio` double DEFAULT NULL,
            `priceToBookRatio` double DEFAULT NULL,
            `priceToSalesRatio` double DEFAULT NULL,
            `priceEarningsRatio` double DEFAULT NULL,
            `priceToFreeCashFlowsRatio` double DEFAULT NULL,
            `priceToOperatingCashFlowsRatio` double DEFAULT NULL,
            `priceCashFlowRatio` double DEFAULT NULL,
            `priceEarningsToGrowthRatio` double DEFAULT NULL,
            `priceSalesRatio` double DEFAULT NULL,
            `dividendYield` double DEFAULT NULL,
            `enterpriseValueMultiple` double DEFAULT NULL,
            `priceFairValue` double DEFAULT NULL,
            `ticker` text,
            CONSTRAINT UC_date_ticker UNIQUE (date, ticker(20)),
            INDEX (ticker(20))
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;"""
    )


def drop_table(engine, inspector, table_name):
    if inspector.has_table(table_name):
        engine.execute(f"DROP TABLE {table_name};")


def __preprocess_daily_df(daily_df):
    import numpy as np

    daily_df.date = daily_df.date.astype("datetime64[D]")
    daily_df.drop(columns=["changeOverTime", "label"], inplace=True)
    daily_df.replace([np.inf, -np.inf], np.nan, inplace=True)
    return daily_df.dropna()


def __read_csvs(csv_list, table_name, process_id, verbose):
    import sqlalchemy as sqla
    import pandas as pd
    import os

    engine = sqla.create_engine(
        "mysql+pymysql://user:user@localhost:3306/stock_data", echo=False
    )

    inspector = sqla.inspect(engine)
    if not inspector.has_table(table_name):
        print(f"table: {table_name} does not exist.")
        return

    temp_table = f"temp_table_{process_id}"
    for i, csv in enumerate(csv_list):
        df = pd.read_csv(csv)
        file_name = os.path.split(csv_list[i])[-1]
        ticker = file_name.split("_")[0]
        df["ticker"] = ticker

        if table_name == "daily":
            df = __preprocess_daily_df(df)

        df.to_sql(temp_table, con=engine, index=False, if_exists="replace")
        engine.execute(f"INSERT IGNORE INTO {table_name} SELECT * FROM {temp_table};")
        engine.execute(f"DROP TABLE {temp_table};")
        if verbose and i % 100 == 0:
            print(f"process {process_id} read csv {i}/{len(csv_list)}")

    print(f"process {process_id} finished.")


def read_csvs_into_table(csv_dir, table_name, num_processes=1, verbose=True):
    data_dir = os.path.join(os.getcwd(), os.path.abspath(csv_dir))

    csv_file_names = [
        os.path.join(data_dir, name)
        for name in os.listdir(data_dir)
        if name[-4:] == ".csv"
    ]
    if num_processes > 1 and len(csv_file_names) > 50:
        csv_per_process = len(csv_file_names) // num_processes
        for i in range(num_processes):
            csv_batch = csv_file_names[
                i * csv_per_process : (i + 1) * csv_per_process + 1
            ]
            process = mp.Process(
                target=__read_csvs, args=(csv_batch, table_name, i, verbose)
            )
            process.start()

    else:
        __read_csvs(csv_file_names, table_name, 0, verbose)


def download_all_stock_csvs(engine, inspector, csv_type="ratios"):
    if inspector.has_table("tickers"):
        tickers = engine.execute("SELECT ticker from tickers").fetchall()
        tickers = [t[0] for t in tickers]

        data_interface = fmp.FMPCloudInterface(tickers)
        data_interface.pull_data(csv_type)


if __name__ == "__main__":
    import pandas as pd
    import sqlalchemy as sqla
    import numpy as np
    import os
    import sys
    import chardet
    import fmpcloud_interface as fmp
    import multiprocessing as mp

    # engine = sqla.create_engine(
    #     "mysql+pymysql://user:user@localhost:3306/stock_data", echo=False
    # )
    # initialize_ratios_table(engine)
    read_csvs_into_table('./data/ratios', 'ratios', 6)
