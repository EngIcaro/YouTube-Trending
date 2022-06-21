#%%
import pandas as pd
import seaborn as sns
import logging
from create_data_base import read_trending_data


#%%
# ChannelTitle 1
# Description 51719
def check_nan_values(data_base):
    """This function checks if any column has a nan values

    Args:
        data_base (dataframe): database
    """
    for column in data_base:
        print(column,' ', data_base[column].isnull().values.any())
        print(data_base[column].isnull().sum())

# All columns has repeted values
def check_unique_values(data_base):
    """This functions checks if any column has repeted values

    Args:
        data_base (dataframe): database
    """
    for column in data_base:
        values_list = data_base[column].value_counts().values
        for values in values_list:
            if values > 1:
                print(column, ' Has repeted values')
                break


def explore_data_base(data_base):
    """This functions explores some columns in the database

    Args:
        data_base (dataframe): database
    """

    print(data_base.info())

    print(data_base['view_count'].describe())
    print(max(data_base['view_count']))

    print(data_base['likes'].describe())
    print(max(data_base['likes']))
    print(data_base[data_base['likes'] == max(data_base['likes'])])

    print(data_base['dislikes'].describe())
    print(max(data_base['dislikes']))
    print(data_base[data_base['dislikes'] == max(data_base['dislikes'])])

    print(data_base['comment_count'].describe())
    print(max(data_base['comment_count']))
    print(data_base[data_base['comment_count'] == max(data_base['comment_count'])])

    print(data_base['thumbnail_link'].describe())

    print(data_base['comments_disabled'].describe())

    print(data_base['ratings_disabled'].describe())

    print(data_base['description'].describe())
#%%
data_base = read_trending_data("data/")
#%%
check_nan_values(data_base)

#%%
check_unique_values(data_base)

#%%
explore_data_base(data_base)
# %%
