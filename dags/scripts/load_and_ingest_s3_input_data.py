import logging
import os
import pandas as pd

from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.models import (
    Variable,
)


def rename_file(name: str, new_name: str) -> None:
    file_path = '/'.join(name.split('/')[:-1])
    new_path = f'{file_path}/{new_name}'
    os.rename(src=name, dst=new_path)
    return new_path


def load_s3_input_data(ti) -> None:
    
    # boto3 init
    s3_hook = S3Hook(aws_conn_id = 's3_bucket')
    bucket_name = Variable.get('bucket_name')
    path_to_download = '/opt/airflow/'

    # get list of input files 
    list_of_input_files = ti.xcom_pull(task_ids='check_for_new_input_data', key='files_to_load')
    logging.info(f's3 keys {list_of_input_files}')
    
    # load input files 
    # they are small and there are not many files, so without parallelization 
    input_file_names = []
    for key in list_of_input_files:
        file_name = s3_hook.download_file(key, bucket_name, path_to_download)
        input_file_names.append(rename_file(file_name, key.split('/')[-1]))
        logging.info(f'Saved: {input_file_names[-1]}')

    ti.xcom_push(key='input_file_paths', value=input_file_names)
    


def ingest_s3_input_data(ti) -> None:
    input_file_paths = ti.xcom_pull(task_ids='load_s3_input_data', key='input_file_paths')

    # files are quite small so I read them all at ones
    df = pd.concat([pd.read_csv(path, on_bad_lines='skip', encoding='latin-1') for path in input_file_paths])
    
    df = df.rename(
        lambda column_name: column_name.lower().replace(' ', '_').replace('#', '_').strip('_'),
        axis='columns'
    )

    logging.info('Starting ingestion into database...')

    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql('silver_target_molecules', engine, if_exists='append')

    # write names of processed files to DB
    input_file_names = ti.xcom_pull(task_ids='check_for_new_input_data', key='files_to_load')
    processed_files = pd.DataFrame(input_file_names, columns=['file_names'])
    processed_files.to_sql('names_of_loaded_s3_files', engine, if_exists='append')

    logging.info('Data successfully ingested')

    
