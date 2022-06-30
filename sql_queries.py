staging_youtube_drop   = "DROP TABLE IF EXISTS staging_youtube"
staging_category_drop  = "DROP TABLE IF EXISTS staging_category"
dim_categorys_drop     = "DROP TABLE IF EXISTS dim_categorys"
dim_videos_drop        = "DROP TABLE IF EXISTS dim_videos"
dim_channels_drop      = "DROP TABLE IF EXISTS dim_channels"
dim_calendar_drop      = "DROP TABLE IF EXISTS dim_calendar"
fact_trendings_drop    = "DROP TABLE IF EXISTS fact_trending"


# ------ CREATE TABLES ---------- #
staging_youtube_create = ("""CREATE TABLE IF NOT EXISTS staging_youtube
                                (
                                    staging_youtube_id_seq SERIAL PRIMARY KEY ,
                                    video_id               varchar NOT NULL   ,
                                    title                  varchar            ,
                                    published_at           varchar NOT NULL   ,
                                    channel_id             varchar NOT NULL   ,
                                    channel_title          varchar            ,
                                    category_id            smallint           ,
                                    trending_date          varchar            ,
                                    tags                   varchar            ,
                                    view_count             integer            ,
                                    likes                  integer            ,
                                    dislikes               integer            ,
                                    comment_count          integer            ,
                                    thumbnail_link         varchar            ,
                                    comments_disabled      varchar            ,
                                    rating                 varchar            ,
                                    description            varchar            ,
                                    country                varchar
                                );
                        """)

staging_category_create = ("""CREATE TABLE IF NOT EXISTS staging_category
                                (
                                    staging_category_id_seq SERIAL PRIMARY KEY ,
                                    category_id             smallint           ,  
                                    category_title          varchar            ,
                                    country                 varchar            

                                );
                        """)

dim_categorys_create    = ("""CREATE TABLE IF NOT EXISTS dim_categorys
                                (
                                    categorys_category_id    smallint           ,
                                    categorys_category_title varchar            ,
                                    categorys_country        varchar            ,
                                    PRIMARY KEY (categorys_category_id, categorys_country)           
                                );

                        """)

dim_videos_create       = ("""CREATE TABLE IF NOT EXISTS dim_videos
                                (
                                    videos_video_id               varchar PRIMARY KEY,
                                    videos_title                  varchar            ,
                                    videos_published_at           timestamp NOT NULL   ,
                                    videos_tags                   varchar            ,
                                    videos_thumbnail_link         varchar            ,
                                    videos_comments_disabled      varchar            ,
                                    videos_rating                 varchar            ,
                                    videos_description            varchar            
                                )
                        """)

dim_channels_create     = ("""CREATE TABLE IF NOT EXISTS dim_channels
                                (
                                    channels_channel_id           varchar PRIMARY KEY,
                                    channels_channel_title        varchar
                                )                       
                        """)

dim_calendar_create     = ("""CREATE TABLE IF NOT EXISTS dim_calendar
                                (
                                    calendar_trending_date      timestamp PRIMARY KEY ,
                                    calendar_day                int         ,
                                    calendar_week               int         ,
                                    calendar_month              int         ,
                                    calendar_year               int 
                                )


                        """)


fact_trendings_create   = ("""CREATE TABLE IF NOT EXISTS fact_trending
                                (
                                    fact_trending_id_seq   SERIAL PRIMARY KEY ,
                                    video_id               varchar NOT NULL   ,
                                    channel_id             varchar NOT NULL   ,
                                    category_id            smallint           ,
                                    trending_date          timestamp          ,
                                    view_count             integer            ,
                                    likes_count            integer            ,
                                    dislikes_count         integer            ,
                                    comment_count          integer            ,
                                    country                varchar            ,
                                    FOREIGN KEY (category_id,country) REFERENCES dim_categorys (categorys_category_id,categorys_country),
                                    FOREIGN KEY (video_id) REFERENCES dim_videos(videos_video_id),
                                    FOREIGN KEY (channel_id) REFERENCES dim_channels (channels_channel_id),
                                    FOREIGN KEY (trending_date) REFERENCES dim_calendar (calendar_trending_date)
                                );
                        """)


# ----- INSERT RECORDS ------ #
staging_youtube_insert = ("""INSERT INTO staging_youtube
                            (
                                video_id               ,
                                title                  ,
                                published_at           ,
                                channel_id             ,
                                channel_title          ,
                                category_id            ,
                                trending_date          ,
                                tags                   ,
                                view_count             ,
                                likes                  ,
                                dislikes               ,
                                comment_count          ,
                                thumbnail_link         ,
                                comments_disabled      ,
                                rating                 ,
                                description            ,
                                country
                            )
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        """)


staging_category_insert = ("""INSERT INTO staging_category
                            (
                                category_id    ,
                                category_title ,
                                country
                            )
                            VALUES(%s,%s,%s)
                        """)


dim_categorys_insert    = ("""INSERT INTO dim_categorys (categorys_category_id, categorys_category_title,
                                                         categorys_country)
                            SELECT DISTINCT
                                category_id     ,
                                category_title  ,
                                country

                            FROM staging_category

                            UNION ALL 

                            SELECT DISTINCT
                                category_id          ,
                                '' as category_title ,
                                country

                            FROM staging_youtube
                            WHERE NOT EXISTS (SELECT 1 FROM staging_category WHERE (staging_category.category_id = staging_youtube.category_id and
                                              staging_category.country = staging_youtube.country));
                        """)

dim_videos_insert       = ("""INSERT INTO dim_videos (videos_video_id, videos_title, videos_published_at,
                                                      videos_tags, videos_thumbnail_link, videos_comments_disabled,
                                                      videos_rating,videos_description)
                            
                            SELECT DISTINCT ON
                                    (video_id) video_id ,
                                    title            ,
                                    to_date(published_at, 'YYYY-MM-DD'), 
                                    tags             ,
                                    thumbnail_link   ,
                                    comments_disabled,
                                    rating           ,
                                    description      
                            FROM staging_youtube ;

                        """)

dim_channels_insert     = ("""INSERT INTO dim_channels (channels_channel_id,channels_channel_title)
                        
                            SELECT DISTINCT ON 
                                (channel_id) channel_id ,
                                channel_title
                            
                            FROM staging_youtube;
                            
                        """) 

dim_calendar_insert     = (""" INSERT INTO dim_calendar (calendar_trending_date,calendar_day,
                                                     calendar_week,calendar_month,calendar_year)

                                SELECT 
                                    query_aux.trending_date_aux					 ,
                                    EXTRACT(day FROM query_aux.date_aux)     ,
                                    EXTRACT(weeks  FROM query_aux.date_aux)  ,	
                                    EXTRACT(month FROM query_aux.date_aux)   ,		
                                    EXTRACT(year FROM query_aux.date_aux)      
                                    

                                FROM (SELECT DISTINCT 
                                            to_date(trending_date, 'YYYY-MM-DD') as trending_date_aux,
                                            to_date(trending_date, 'YYYY-MM-DD') AS date_aux
                                            FROM staging_youtube
                                    ) AS query_aux ;

                        """)


fact_trendings_insert   = ("""INSERT INTO fact_trending (video_id, channel_id, category_id,
                                                         trending_date, view_count, likes_count,
                                                         dislikes_count, comment_count, country)
                            SELECT 
                                    video_id               ,
                                    channel_id             ,
                                    category_id            ,
                                    to_date(trending_date, 'YYYY-MM-DD') ,          
                                    view_count             ,
                                    likes                  ,
                                    dislikes               ,
                                    comment_count          ,
                                    country      
                            FROM staging_youtube;           
                        """)

create_table_queries = [staging_youtube_create, staging_category_create, dim_categorys_create, dim_channels_create,dim_videos_create,dim_calendar_create, fact_trendings_create]
drop_table_queries   = [staging_youtube_drop,staging_category_drop,fact_trendings_drop,dim_categorys_drop, dim_videos_drop, dim_channels_drop, dim_calendar_drop]
insert_table_queries = [dim_categorys_insert,dim_videos_insert,dim_channels_insert,dim_calendar_insert,fact_trendings_insert]