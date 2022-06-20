#%%
from sys import path_hooks
import pandas as pd
import seaborn as sns
# %%
# https://www.kaggle.com/datasets/rsrishav/youtube-trending-video-dataset?select=US_youtube_trending_data.csv
data_base = pd.read_csv("data/BR_youtube_trending_data.csv")
# %%
print(data_base.info())

#%% [vídeo_id]
# Question: video_id is Unique? Count distinct video_id
print(data_base['video_id'].value_counts())
# Answer: No. #video can stay more than one day in the trending videos 
print(data_base['video_id'].isna().sum())
# doesn't have nan values

# %% [title]
# Question: title is Unique? Count distinct title
print(data_base['title'].value_counts())
# Answer: No. #title can stay more than one day in the trending videos 
#         OR the same title can be in one or more videos.
# Examples: Você conta ou eu conto? Djonga - Ea$y Money
print(data_base['title'].isna().sum())
# doesn't have nan values

# %%[publishedAt]
# date published vídeo YYYY-MM-DDT00:00:00
print(data_base['publishedAt'])

# %% [channelId]
# channelId can be the same in different videos
print(data_base['channelId'].value_counts())
# doesn't have nan values
print(data_base['channelId'].isna().sum())

#%%[channelTitle]
# channelTitle can be the same in different videos
print(data_base['channelTitle'].value_counts())
# doesn't have nan values
print(data_base['channelTitle'].isna().sum())
channel_title= data_base['channelTitle'].value_counts()

#%%[categoryId]
print(data_base['categoryId'].value_counts())
# doesn't have nan values
print(data_base['categoryId'].isna().sum())
sns.countplot(data_base.categoryId)

#%%[trending_date]
# what is the difference between trending_date and publishedAt
print(data_base['trending_date'])
# %%[tags]
# Question: Who are the most popular tags?
print(data_base['tags'])
# Answer: To-Do
# %%[view_count]
print(data_base['view_count'].describe())
print(max(data_base['view_count']))

# %%
print(data_base['likes'].describe())
print(max(data_base['likes']))
print(data_base[data_base['likes'] == max(data_base['likes'])])

# %%
print(data_base['dislikes'].describe())
print(max(data_base['dislikes']))
print(data_base[data_base['dislikes'] == max(data_base['dislikes'])])


# %%
print(data_base['comment_count'].describe())
print(max(data_base['comment_count']))
print(data_base[data_base['comment_count'] == max(data_base['comment_count'])])


# %%
print(data_base['thumbnail_link'].describe())

#%%
print(data_base['comments_disabled'].describe())
# %%

print(data_base['ratings_disabled'].describe())
# %%

print(data_base['description'].describe())
# %%
