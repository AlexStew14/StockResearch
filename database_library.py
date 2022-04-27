
import pandas as pd
import sqlalchemy as sqla


class DatabaseLibrary:
    def __init__(self):
        self.engine = sqla.create_engine('mysql+pymysql://user:user@localhost:3306/stock_data', echo=False)    
        self.inspector = sqla.inspect(self.engine)

        
    def get_rows_by_tickers(self, table_name, tickers, date_begin=None, date_end=None):
        if not self.inspector.has_table(table_name):
            raise Exception('table_name not found in the database.')

        tickers_str = ','.join([f"'{t}'" for t in tickers]) 
        query = f"SELECT * FROM {table_name} WHERE ticker in ({tickers_str})"

        if date_begin is not None and date_end is not None:
            date_begin = "'" + date_begin + "'"
            date_end = "'" + date_end + "'"
            query += f" and date >= {date_begin} and date <= {date_end}"

        query += " order by ticker,date asc;"

        query_obj = self.engine.execute(query)

        column_names = list(query_obj.keys())
        res_list = query_obj.fetchall()
        
        res_df = pd.DataFrame(res_list, columns=column_names)
        return res_df

    def get_rows_by_days(self, table_name, starting_date, last_days=5):
        # date format: 'YYYY-MM-DD'
        if not self.inspector.has_table(table_name):
            raise Exception('table_name not found in the database.')

        query = f"SELECT * FROM {table_name} WHERE date >= DATE_SUB(date('{starting_date}'), INTERVAL {last_days} DAY);"

        query_obj = self.engine.execute(query)

        column_names = list(query_obj.keys())
        res_list = query_obj.fetchall()
        
        res_df = pd.DataFrame(res_list, columns=column_names)
        return res_df
