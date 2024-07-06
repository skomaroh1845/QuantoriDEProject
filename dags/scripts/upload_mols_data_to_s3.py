import logging

from airflow.hooks.S3_hook import S3Hook
from airflow.models import (
    Variable,
)


def upload_mols_data(ti) -> None:
    
    # boto3 init
    s3_hook = S3Hook(aws_conn_id = 's3_bucket')
    bucket_name = Variable.get('bucket_name')
    output_prefix = Variable.get('output_prefix')  # ends with /

    input_file_names = ti.xcom_pull(task_ids='load_s3_input_data', key='input_file_names')
    key = output_prefix + input_file_names[0].split('/')[-1]

    s3_hook.load_file(input_file_names[0], key, bucket_name)
    logging.info(f'loaded: key={key}, file={input_file_names[0]}')



