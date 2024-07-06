import logging
import requests
import json
from airflow.models import (
    Variable,
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2


# returns value of latest uploaded chunk's offset
def _fetch_last_chunk_offset() -> int:

    # get latest offset
    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    connection = postgres_hook.get_conn()
    with connection.cursor() as cursor:

        # check if a table is empty
        try:
            cursor.execute(
                'select case when exists(select 1 from nkomarov.bronze_raw_mols_data) then 0 else 1 end as IsEmpty;'
            )  # 1 - empty, 0 - full

            if cursor.fetchall()[0][0] == 1:
                return 0  # refers to an empty of not exist table
            
        except psycopg2.errors.UndefinedTable as UndefinedTable:
            return 0  

        # check for missed offsets
        # this query compares a raw with a previous raw and checks if the difference is more than chunk size
        cursor.execute(
            """
            select tmp."skipped_offset" 
            from
                (select "chunk_size" + lag("offset") over (ORDER BY "offset") as "skipped_offset",
                    "offset" - "chunk_size" - lag("offset") over (ORDER BY "offset") as "skepped_value"
                from bronze_raw_mols_data) tmp
            where tmp."skepped_value" > 0
            order by tmp."skipped_offset";
            """
        ) # [(value,)]
        
        if len(cursor.fetchall()) > 0:
            # missed offset found 
            # (the lowest offset of lastest loaded chunk from the slowest worker)
            return cursor.fetchall()[0][0]  
        else:
            # no missed offsets found
            # than last offset is max offset 
            cursor.execute('select max("offset") from nkomarov.bronze_raw_mols_data;')
            return cursor.fetchall()[0][0] 



def check_chembl_data_existance() -> None:
    
    chembl_url = Variable.get('chembl_url')

    response = requests.get(chembl_url, params={'format': 'json', 'limit': 1, 'offset': 0})
    if response.status_code == 200:
        content_json = json.loads(response.content.decode('utf-8'))
        metadata = content_json['page_meta']
    else:
        logging.error('Problems with getting source data metadata')
        raise Exception()

    total_count = metadata['total_count']
    last_chunk_offset = _fetch_last_chunk_offset()
    chunk_size = int(Variable.get('chunk_size'))

    logging.info(f'last chunk: {last_chunk_offset}, total count: {total_count}')

    if total_count - last_chunk_offset < chunk_size:
        return 'load_s3_input_data'
    else:
        return 'handle_chembl_data'