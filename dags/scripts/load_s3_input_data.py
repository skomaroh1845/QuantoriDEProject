import logging
import os

from airflow.hooks.S3_hook import S3Hook
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
    input_prefix = Variable.get('input_prefix')  # ends with /
    path_to_download = '/opt/airflow/'

    # get list of input files 
    list_of_inlut_files = s3_hook.list_keys(bucket_name=bucket_name, prefix=input_prefix)
    logging.info(f's3 keys {list_of_inlut_files}')
    
    # load input files 
    # they are small and there are not many files, so without parallelization 
    input_file_names = []
    for key in list_of_inlut_files:
        if key.split('/')[-1] != '':
            file_name = s3_hook.download_file(key, bucket_name, path_to_download)   # S3Hook.get_conn().download_file()
            input_file_names.append(rename_file(file_name, key.split('/')[-1]))
            logging.info(f'Saved: {input_file_names[-1]}')

    ti.xcom_push(
        key='input_file_names',
        value=input_file_names
    )
    

