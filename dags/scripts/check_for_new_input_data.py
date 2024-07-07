import logging
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2
from airflow.models import (
    Variable,
)



def check_for_new_input_data(ti) -> str:
    # boto3 init
    s3_hook = S3Hook(aws_conn_id = 's3_bucket')
    bucket_name = Variable.get('bucket_name')
    input_prefix = Variable.get('input_prefix')  # ends with /

    # get list of input files 
    list_of_input_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=input_prefix)
    list_of_input_files.remove(input_prefix)  # del a 'folder' name from list of files 

    logging.info(f"Got input files list from s3: {list_of_input_files}")

    # get list of ingested files
    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    connection = postgres_hook.get_conn()
    with connection.cursor() as cursor:
        try:
            cursor.execute(
                'select * from names_of_loaded_s3_files;'
            )

            loaded_files = [ _[1] for _ in cursor.fetchall()]
            
        except psycopg2.errors.UndefinedTable as UndefinedTable:
            loaded_files = []
    logging.info(f"Got ingested files list: {loaded_files}")

    # compare 
    new_files = list(set(list_of_input_files).difference(set(loaded_files)))

    if len(new_files) > 0:
        # pass new files to loader
        logging.info(f"Found new input files: {new_files}")
        ti.xcom_push(key='files_to_load', value=new_files)
        return 'load_s3_input_data'
    else:
        # no new input -> finish
        logging.info(f"No new input files found.")
        return 'finish'



