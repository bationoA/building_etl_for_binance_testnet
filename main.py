import pandas as pd

from functions import *
from datetime import datetime
import os
from config import config

from sqlalchemy import create_engine

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    if not table_exist(table_name=config.get('MAIN_TABLE_NAME')):
        df = fetch_btc_n_data(n=5)  # here, n is arbitrary, just for creating the table
        init_database(df=df)
    else:
        df = pd.DataFrame()
        # If table exists
        last_open_time = get_most_recent_open_time()
        print(f"last_open_time: {last_open_time}")
        if last_open_time is None:
            # If the table is empty then
            df = fetch_btc_n_data(n=config.get('NUMBER_ROWS_PER_REQUEST'),
                                  ref_time_type='start_time')
        else:
            # If te table is not empty
            df = fetch_btc_n_data(n=config.get('NUMBER_ROWS_PER_REQUEST'),
                                  ref_time_type='end_time',
                                  end_time_in_sec=last_open_time/1000)

            # print("df")
            # print(df)

            df = df.loc[df['Open_time'] > last_open_time]  # filter out the row with existing Open_time

        for index, row in df.iterrows():
            row_df = pd.DataFrame([row])
            # Generate the SQL code for insertion
            insert_sql = generate_insert_sql(table_name=config.get('MAIN_TABLE_NAME'), dataframe=row_df)
            # Execute the SQL code for insertion
            result = execute_sql_query(sql_query=insert_sql, execution_type="execute")

    try:
        current_file_path = os.path.abspath(__file__)
        directory = os.path.dirname(current_file_path)
        file_path = os.path.join(directory, "last_check.json")
        with open(file_path, "w") as json_file:
            json.dump(datetime.now().strftime("%B %d, %Y %H:%M"), json_file)
    except Exception as e:
        print("Error:", e)
