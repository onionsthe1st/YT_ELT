from airflow.providers.postgres.hooks.postgres import PostgresHook
from pscycopg2.extras import RealDictCursor


table = "video_stats"
def get_conn_cursor():
    hook = PostgresHook(postgres_conn_id="postgres_db_yt_elt", database="elt_db")
    conn = hook.get_conn()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    return conn, cursor

def close_conn_cursor(conn, cursor):
    cursor.close()
    conn.close()

def create_schema( schema ):
    conn, cursor = get_conn_cursor()
    schema_sql = f"CREATE SCHEMA IF NOT EXISTS {schema}"
    cursor.execute(schema_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)
 
def create_table( schema ):
    conn, cursor = get_conn_cursor()
    if schema == "staging":
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                video_id VARCHAR(255) PRIMARY KEY,
                channel_title TEXT NOT NULL,
                title TEXT NOT NUL,
                description TEXT NOT NUL,
                duration VARCHAR(50),
                publishedAt TIMESTAMP,
                view_count INT,
                like_count INT,
                comment_count INT,
                definition VARCHAR(20)
            );
            """
    else:
        table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                video_id VARCHAR(255) PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL,
                duration TIME,
                video_type VARCHAR(50), 
                publishedAt TIMESTAMP,
                view_count INT,
                like_count INT,
                comment_count INT,

            );
            """    
    cursor.execute(table_sql)
    conn.commit()
    close_conn_cursor(conn, cursor)

def get_video_ids(cursor,schema):
    
    query = f"SELECT video_id FROM {schema}.{table}"
    cursor.execute(query)
    ids = cursor.fetchall()
    video_ids = [row["video_id"] for row in ids]
    
    return video_ids