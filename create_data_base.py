#%%
import pandas as pd
import os
import json
import logging
#%%

def read_trending_data(files_path) -> pd.DataFrame:
    """This function reads all youtube trending data 
       and returns in the pandas  dataframe. A new column
       called country was created in the dataframe

    Args:
        files_path (string): path of files

    Returns:
        pd.DataFrame: Dataframe with all Youtube data
    """

    logging.info('executting read_trending_data')
    dataframes_list = []

    for filename in os.listdir(files_path):
        if filename.endswith('.csv'):
            aux_dataframe = pd.read_csv(f"{files_path}{filename}")
            aux_dataframe['country'] = filename.split("_")[0]
            dataframes_list.append(aux_dataframe)

    return pd.concat(dataframes_list)


#%%

def read_category_data(file_path) -> pd.DataFrame:
    """this function reads all category json files and returns 
        them in dataframe. A new column called country was created
        in the dataframe.
    Args:
        file_path (string): path of files

    Returns:
        pd.DataFrame: Dataframe with category data
    """
    logging.info("executting read_category_data")
    dataframe_list = []

    for filename in os.listdir(file_path):
        if filename.endswith('.json'):
            with open(f'{file_path}{filename}') as json_file:
                object_json = json.load(json_file)
            
            list_json = object_json["items"]
            list_aux = []

            for category in list_json:
                list_aux.append(list((category["id"],category["snippet"]["title"], filename.split("_")[0])))

            dataframe_list.append(pd.DataFrame(list_aux, columns=["categoryId", "categoryTitle", "country"]))

    return pd.concat(dataframe_list)


# %%
def remove_nan_rows(data_base, column_name) -> bool:
    """This function removes all rows with nan values in
       column_name columns
    Args:
        data_base (dataframe): database
        column_name (list): list with column names

    Returns:
        bool: True if removed any lines or False otherwise
    """
    before_rows = data_base.shape[0]
    data_base.dropna(axis=0,subset=column_name, inplace=True)
    
    if(before_rows != data_base.shape[0]):
        return True
    
    return False

#%%
#category_data = read_category_data("data/")
#data_base = read_trending_data("data/")
#remove_nan_rows(data_base,['channelTitle'])
# %%
# %%

import psycopg2 as pg
from sqlalchemy import create_engine
# %%
#establishing the connection
conn = pg.connect(
    database="mydb",user='postgres', password='postgres', host='localhost', port= '5432'
)
conn.autocommit = True
# %%
#Creating a cursor object using the cursor() method
cursor = conn.cursor() 
#Preparing query to create a database
#%%
sql = "CREATE database ondeee"
#%%
#Creating a database
cursor.execute(sql)
print("Database created successfully........")
# %%
conn.close()
# %%
cursor.execute("CREATE TABLE WIP_S3 (RMA VARCHAR);")
# %%
cursor.execute("INSERT INTO WIP_S3 (RMA) VALUES('OIEEE2');")
# %%
