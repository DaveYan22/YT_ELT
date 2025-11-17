from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor, create_schema, create_table, get_video_id
from datawarehouse.data_loading import load_path
from datawarehouse.data_modification import insert_rows, update_rows, delete_rows
from datawarehouse.data_transformation import transform_data

import logging
from airflow import DAG
from airflow.decorators import dag, task

logger = logging.getLogger(__name__)
table = "yt_api"

@task
def staging_table():
    schema = 'staging'
    conn, cur = None, None
    try:
        conn, cur = get_conn_cursor()
        YT_data = load_path()
        create_schema(schema)
        create_table(schema)

        table_ids = get_video_id(cur, schema)

        # FIXED: Corrected the indentation - else block now inside for loop
        for row in YT_data:
            if len(table_ids) == 0:
                insert_rows(cur, conn, schema, row)
            else:
                if row['video_id'] in table_ids:
                    update_rows(cur, conn, schema, row)
                else:
                    insert_rows(cur, conn, schema, row)
                
        # Delete rows that are no longer in the JSON
        ids_in_json = {row['video_id'] for row in YT_data}
        ids_to_delete = set(table_ids) - ids_in_json

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)

        logger.info("Staging table processing completed.")
    except Exception as e:
        logger.error(f"Error in staging_table task: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)

@task
def core_table():
    schema = 'core'
    conn, cur = None, None
    
    try:
        conn, cur = get_conn_cursor()

        create_schema(schema)
        create_table(schema)

        table_ids = get_video_id(cur, schema)

        current_video_ids = set()

        cur.execute(f'SELECT * FROM staging.{table};')
        rows = cur.fetchall()

        for row in rows:
            current_video_ids.add(row["video_id"])
    
            if len(table_ids) == 0:
                # If the target table is empty, simply transform and insert
                transformed_row = transform_data(row)
                insert_rows(cur, conn, schema, transformed_row)
            
            else:
                # Transform the data first
                transformed_row = transform_data(row)
                
                # Check if the transformed row's ID exists in the target table's IDs
                if transformed_row["Video_ID"] in table_ids:
                    # If ID exists, update the row
                    update_rows(cur, conn, schema, transformed_row)
                else:
                    # If ID does not exist, insert a new row
                    insert_rows(cur, conn, schema, transformed_row)

        ids_to_delete = set(table_ids) - current_video_ids

        if ids_to_delete:
            delete_rows(cur, conn, schema, ids_to_delete)
            
        logger.info("Core table processing completed.")
    except Exception as e:
        logger.error(f"Error in core_table task: {e}")
        raise e
    finally:
        if conn and cur:
            close_conn_cursor(conn, cur)