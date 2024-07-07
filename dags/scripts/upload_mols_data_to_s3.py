import logging
import os

from airflow.hooks.S3_hook import S3Hook
from airflow.models import (
    Variable,
)


def upload_mols_data(folder_name: str, xcom_pull_task_id:str, xcom_pull_key: str, del_after: bool, ti) -> None:
    
    # boto3 init
    s3_hook = S3Hook(aws_conn_id = 's3_bucket')
    bucket_name = Variable.get('bucket_name')
    output_prefix = Variable.get('output_prefix')  # ends with /
    output_prefix = output_prefix + folder_name

    files_names = ti.xcom_pull(task_ids=xcom_pull_task_id, key=xcom_pull_key)
    
    # if there is one file 
    if not isinstance(files_names, list):
        files_names = [files_names]

    for file in files_names:
        key = output_prefix + file[0].split('/')[-1]
        s3_hook.load_file(file, key, bucket_name)
        logging.info(f'loaded file {file} to S3 bucket {bucket_name}, key: {key}')
        
        if del_after:
            os.remove(file)

