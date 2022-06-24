staging_youtube_drop   = "DROP TABLE IF EXISTS staging_youtube"
staging_category_drop  = "DROP TABLE IF EXISTS staging_category"


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



create_table_queries = [staging_youtube_create, staging_category_create]
drop_table_queries   = [staging_youtube_drop, staging_category_drop]
