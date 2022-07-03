#%%
import pandas as pd
import os
import json
import logging
from tqdm import tqdm
from sql_queries import *
import psycopg2 as pg
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


def process_trending_data(cur, data_base):
    """This function fills staging_youtube table.

    Args:
        cur (_cursor): cursor to database session
        data_base (dataframe): all youtube tranding data
    """
    for index, row in tqdm(data_base.iterrows()):

        youtube_data = (row.video_id      ,       
                        row.title         ,        
                        row.publishedAt   ,        
                        row.channelId     ,
                        row.channelTitle  ,
                        row.categoryId    ,
                        row.trending_date ,
                        row.tags          ,
                        row.view_count    ,
                        row.likes         ,
                        row.dislikes      ,
                        row.comment_count ,
                        row.thumbnail_link,
                        row.comments_disabled ,
                        row.ratings_disabled  ,
                        row.description       ,
                        row.country
                        )
        cur.execute(staging_youtube_insert, youtube_data)


def process_category_data(cur, category_data):
    """This function fills staging_category table.

    Args:
        cur (_cursor): cursor to database session
        category_data (dataframe): all category data
    """
    for index, row in tqdm(category_data.iterrows()):

        category_data=(
                        row.categoryId    ,
                        row.categoryTitle , 
                        row.country

                    )

        cur.execute(staging_category_insert, category_data)

def insert_tables(cur):
    """This function fills all dimensions and fact tables 

    Args:
        cur (_cursor): cursor to database session
    """
    for query in insert_table_queries:
        cur.execute(query)

def run_data_quality_checks(cur):
    """This function checking the quality of final data

    Args:
        cur (_cursor): cursor to database session
    """
    for quality in data_quality_checks:
        cur.execute(quality['check_sql'])
        result = cur.fetchone()
        result = result[0]
        if(quality['expected_type'] == 'number'):
            if(result == quality['expected_result']):
                print("Data Quality [OK]")
            else:
                print("Data Quality [Error]")
        else:
            cur.execute(quality['expected_result'])
            except_result = cur.fetchone()
            except_result = except_result[0]
            if(except_result == result):
                print("Data Quality [OK]")
            else:
                print("Data Quality [Error]")



def main():

    # connect to youtube database
    conn = pg.connect(
        database="youtubedb",user='postgres', password='postgres', host='localhost', port= '5432'
    )
    cursor = conn.cursor()
    conn.autocommit = True


    # get youtube trending_data
    data_base = read_trending_data("data/")
    # get category_data
    category_data = read_category_data("data/")
    # Remove nan rows of the trending_Data 
    remove_nan_rows(data_base,['channelTitle'])

    # insert youtube_tranding_data in db
    process_trending_data(cursor, data_base)
    # insert category_data in db
    process_category_data(cursor,category_data)

    # insert olap tables
    insert_tables(cursor)

    # Run Data Quality Checks
    run_data_quality_checks(cursor)
#%%
if __name__ == "__main__":
    main()
