import os
import glob
import sqlite3
import warnings
import datetime
import pandas as pd
import measure_time
from work_with_folders import get_name_folder

warnings.simplefilter("ignore")

@measure_time.measure_time
def create_database_from_eirs_(path):
    source_file = glob.glob(f'{path}/*.xlsx')
    print(source_file)

    name_database = get_name_folder(path)
    print(name_database)

    for root, dirs, files in os.walk('../lib/database_from_eirs'):
        print(root)
        print(dirs)
        print(files)

    date_fmt = datetime.datetime.now()
    gen_date_and_time = f"__{date_fmt.date()}_{date_fmt.hour}_{date_fmt.minute}_{date_fmt.second}"

    db = sqlite3.connect(f'../lib/database_from_eirs/{name_database}_{gen_date_and_time}.db')
    dfs = pd.read_excel(source_file[0], sheet_name=None)
    for table, df in dfs.items():
        df.to_sql(table, db)


create_database_from_eirs_('../lib/database_from_eirs')
