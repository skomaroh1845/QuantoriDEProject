import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.models import (
    Variable,
)


def upload_mols_data(xcom_pull_task_id:str, xcom_pull_key: str, ti) -> None:
    
    # boto3 init
    s3_hook = S3Hook(aws_conn_id = 's3_bucket')
    bucket_name = Variable.get('bucket_name')
    output_prefix = Variable.get('output_prefix')  # ends with /

    fp_files_names = ti.xcom_pull(task_ids='calc_mols_fingerprints', key='fp_files_names')

    for file in fp_files_names:
        key = output_prefix + file[0].split('/')[-1]
        s3_hook.load_file(file, key, bucket_name)
        logging.info(f'loaded file {file} to S3 bucket {bucket_name}, key: {key}')

