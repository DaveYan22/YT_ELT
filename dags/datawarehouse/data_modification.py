import logging

logger = logging.getLogger(__name__)
table = "yt_api"


def insert_rows(cur, con, schema, row):
    try:
        if schema == "staging":
            video_id = "video_id"
            # Use lowercase column names for staging (PostgreSQL default)
            cur.execute(
                f"""
            INSERT INTO {schema}.{table} (video_id, title, publishedat, duration, viewcount, likecount, commentcount)
            VALUES (%(video_id)s, %(title)s, %(publishedAt)s, %(duration)s, %(viewCount)s, %(likeCount)s, %(commentCount)s);
            """,
                row,
            )

        else:
            video_id = "Video_ID"
            cur.execute(
                f"""
            INSERT INTO {schema}.{table} ("Video_ID", "Video_Title", "Upload_Date", "Duration", "Video_Type", "Video_Views", "Likes_Count", "Comments_Count")
            VALUES (%(Video_ID)s, %(Video_Title)s, %(Upload_Date)s, %(Duration)s, %(Video_Type)s, %(Video_Views)s, %(Likes_Count)s, %(Comments_Count)s);
            """,
                row,
            )

        con.commit()
        logger.info(f"Inserted video ID: {row[video_id]}")

    except Exception as e:
        logger.error(f"Error inserting video ID {row[video_id]}: {e}")
        raise e


def update_rows(cur, con, schema, row):
    try:
        if schema == "staging":
            # Column names in the staging table (lowercase)
            video_id_col = "video_id"
            upload_date_col = "publishedat"
            video_title_col = "title"
            video_views_col = "viewcount"
            likes_count_col = "likecount"
            comments_count_col = "commentcount"
            
            # Dictionary keys from the row data
            video_id_key = "video_id"
            upload_date_key = "publishedAt"
            video_title_key = "title"
            video_views_key = "viewCount"
            likes_count_key = "likeCount"
            comments_count_key = "commentCount"
            
            cur.execute(
                f"""UPDATE {schema}.{table}
                SET {video_title_col} = %s,
                    {upload_date_col} = %s,
                    {video_views_col} = %s,
                    {likes_count_col} = %s,
                    {comments_count_col} = %s
                WHERE {video_id_col} = %s;
                """,
                (row[video_title_key], row[upload_date_key], row[video_views_key], 
                 row[likes_count_key], row[comments_count_key], row[video_id_key])
            )
        else:
            # Production schema with Title Case columns
            cur.execute(
                f"""UPDATE {schema}.{table}
                SET "Video_Title" = %(Video_Title)s,
                    "Upload_Date" = %(Upload_Date)s,
                    "Video_Views" = %(Video_Views)s,
                    "Likes_Count" = %(Likes_Count)s,
                    "Comments_Count" = %(Comments_Count)s
                WHERE "Video_ID" = %(Video_ID)s;
                """,
                row,
            )
        
        con.commit()
        logger.info(f"Updated video ID: {row.get(video_id_key if schema == 'staging' else 'Video_ID', 'unknown')}")

    except Exception as e:
        logger.error(f"Error updating video ID {row.get('video_id' if schema == 'staging' else 'Video_ID', 'unknown')}: {e}")
        raise e


def delete_rows(cur, conn, schema, ids_to_delete):
    try:
        placeholders = ','.join(['%s'] * len(ids_to_delete))
        
        cur.execute(
            f"""
            DELETE FROM {schema}.{table}
            WHERE "Video_ID" IN ({placeholders});
            """,
            tuple(ids_to_delete)
        )
        
        conn.commit()
        logger.info(f"Deleted {len(ids_to_delete)} rows with Video_IDs")

    except Exception as e:
        logger.error(f"Error deleting rows: {e}")
        raise e