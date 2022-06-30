#%%
import psycopg2 as pg
from sql_queries import create_table_queries, drop_table_queries
# %%
def create_database():
 
    """This function is responsible for creates and connects to the 
       youtubedb. As also Returns the connection and cursor to 
       youtubedb

    Returns:
        tuple[pg.cursor, pg.connection]: cursor and connection to
        youtubedb
    """
    #establishing the connection
    #conn = pg.connect(
    #    database="postgres",user='postgres', password='postgres', host='localhost', port= '5432'
    #)
    #conn.autocommit = True
    #cursor = conn.cursor() 

    # create youtube database with UTF8
#    cursor.execute("DROP DATABASE IF EXISTS youtubedb")
#    cursor.execute("CREATE DATABASE youtubedb WITH ENCODING 'utf8'")

#    conn.close()

    conn = pg.connect(
        database="youtubedb",user='postgres', password='postgres', host='localhost', port= '5432'
    )

    cursor = conn.cursor()
    conn.autocommit = True

    # close connection to default database
    return cursor, conn



def drop_tables(cur):

    for query in drop_table_queries:
        cur.execute(query)


def create_tables(cur):

    for query in create_table_queries:
        cur.execute(query)


def main():

    cur, conn = create_database()
    
    drop_tables(cur)
    create_tables(cur)

    conn.close()


#%%   
if __name__ == "__main__":
    main()

# %%
