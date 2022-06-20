#%%
import pandas as pd
import os
import json
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
    dataframes_list = []

    for filename in os.listdir(files_path):
        if filename.endswith('.csv'):
            aux_dataframe = pd.read_csv(f"{files_path}{filename}")
            aux_dataframe['country'] = filename.split("_")[0]
            dataframes_list.append(aux_dataframe)

    return pd.concat(dataframes_list)


data_base = read_trending_data("data/")
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

category_data = read_category_data("data/")
# %%
