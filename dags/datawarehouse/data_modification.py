
import logging


table = "video_stats"
logger = logging.getLogger(__name__)

def insert_rows(cursor, conn, row, schema):
    try:
        if schema == "staging":

            video_id = "video_id"
            cursor.execute( f"""
                INSERT INTO {schema}.{table} (video_id, channel_title, title, description, duration, publishedAt, view_count, like_count, comment_count, definition)
               VALUES (%(video_id)s, %(channel_title)s, %(title)s, %(description)s, %(duration)s, %(publishedAt)s, %(view_count)s, %(like_count)s, %(comment_count)s, %(definition)s)
                """, row
             )
        else:
            video_id = "video_ID"
            cursor.execute( f"""
                INSERT INTO {schema}.{table} (video_ID, title, description, duration, video_type, publishedAt, view_count, like_count, comment_count)
               VALUES (%(video_ID)s, %(title)s, %(description)s, %(duration)s, %(video_type)s, %(publishedAt)s, %(view_count)s, %(like_count)s, %(comment_count)s)
                """, row
             )
            
        conn.commit()
        logger.info(f"Row with video_ID {row.get(video_id)} inserted successfully.")
    except Exception as e:
        logger.error(f"Error inserting row with video_ID {row.get(video_id)}: {e}")
        raise e
    
def update_rows(cursor, conn, row, schema):
    try:
        if schema == "staging":
            video_id = "video_id"
            title = "title"
            description = "description"
            publishedAt = "publishedAt"
            view_count = "view_count"
            like_count = "like_count"
            comment_count = "comment_count"

                
        else:
            
            video_id = "video_ID"
            title = "title"
            description = "description"
            publishedAt = "publishedAt"
            view_count = "view_count"
            like_count = "like_count"
            comment_count = "comment_count"

        cursor.execute( f"""
            UPDATE {schema}.{table} 
            SET title = %(title)s, description = %(description)s, publishedAt = %(publishedAt)s, view_count = %(view_count)s, like_count = %(like_count)s, comment_count = %(comment_count)s
            WHERE video_id = %(video_id)s AND publishedAt = %(publishedAt)s
            """, row
        )
            
        conn.commit()
        logger.info(f"Row with video_ID {row.get(video_id)} updated successfully.")
    except Exception as e:
        logger.error(f"Error updating row with video_ID {row.get(video_id)}: {e}")
        raise e

def delete_rows(cursor, conn, schema, ids_to_delete):
    try:
        ids_to_delete_str = f"""({', '.join(f"'{video_id}'" for video_id in ids_to_delete)})"""
        cursor.execute( f"""
            DELETE FROM {schema}.{table} 
            WHERE video_id IN ({ids_to_delete_str})
            """)        
        conn.commit()
        logger.info(f"Rows with video_IDs {ids_to_delete} deleted successfully.")
    except Exception as e:
        logger.error(f"Error deleting rows with video_IDs {ids_to_delete}: {e}")
        raise e