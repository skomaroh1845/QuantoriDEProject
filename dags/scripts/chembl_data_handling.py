import logging
import requests
import json
import sqlalchemy
import pandas as pd
import numpy as np
import psycopg2
from time import time 
from typing import Tuple, List, Dict, Any
import multiprocessing


from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import (
    Variable,
)

NECESSARY_COLS = ['molecule_chembl_id', 'molecule_properties', 'molecule_structures', 'molecule_type']  # known by a research of data


def _get_data_chunk(source_url: str, chunk_size: int, offset: int) -> Tuple[List[Dict], Dict]:
    response = requests.get(
        source_url,
        params={
            'format': 'json', 
            'limit': chunk_size, 
            'offset': offset,
        },
    )

    if response.status_code == 200:
        content_json = json.loads(response.content.decode('utf-8'))
        mols_list = content_json['molecules']
        metadata = content_json['page_meta']
        return mols_list, metadata
    else:
        logging.error('Problems with loading data')
        raise Exception()
    

def _process_data_chunk(mols: List[Dict], offset: int) -> pd.DataFrame:
    # convert data to pandas df 
    df = pd.DataFrame(mols)

    # drop unnecessary data (there are cols with nulls mostly)
    cols_to_del = set(df.columns).difference(NECESSARY_COLS)
    df = df.drop(columns=cols_to_del)

    # dump columns with dicts 
    df.molecule_properties = df.molecule_properties.apply(json.dumps)
    df.molecule_structures = df.molecule_structures.apply(json.dumps)
    
    # store offset
    df['offset'] = offset
    return df


def _upload_data_chunk(db_engine: sqlalchemy.engine.base.Engine, df: pd.DataFrame) -> None:
    df.to_sql('bronze_raw_mols_data', db_engine, if_exists='append')


# returns value of latest uploaded chunk's offset
def _fetch_last_chunk_offset() -> int:

    # get latest offset
    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    connection = postgres_hook.get_conn()
    with connection.cursor() as cursor:
        try:
            cursor.execute('SELECT MAX(br."offset") FROM bronze_raw_mols_data br;') # [(value,)]
            latest_offset = cursor.fetchall()[0][0]
        except psycopg2.errors.UndefinedTable as UndefinedTable:
            return 0
     
    if latest_offset is None:
        return 0
    else: 
        return latest_offset


def _worker(args) -> None:
    chembl_url = Variable.get('chembl_url')
    start_point = args[0]
    end_point = args[1]
    chunk_size = args[2]

    current = multiprocessing.current_process()

    # init DB engine for worker
    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    engine = postgres_hook.get_sqlalchemy_engine()

    total_count = end_point - start_point + 1

    # for time estimation
    avg_chunk_time = 0
    all_time = 0
    done_chunks_count = 0
    left_chunks = int(total_count / chunk_size)

    # load/process/ingest loop
    for offset in range(start_point, end_point+chunk_size, chunk_size):
        
        logging.info(
            f'Worker-{current._identity[0]} is handling {done_chunks_count}/{int(total_count / chunk_size)} molecules chunk. ' +
            f'Estimated time (in sec) to finish: {avg_chunk_time * left_chunks}. ' +
            f'Time left (in sec): {int(all_time)}'
        )
        
        curr_time = time()
        mols, _ = _get_data_chunk(chembl_url, chunk_size, offset)
        
        processed_df = _process_data_chunk(mols, offset)
        
        _upload_data_chunk(engine, processed_df)

        # time estimation
        done_chunks_count += 1
        left_chunks -= 1
        process_time = time() - curr_time
        all_time += process_time
        avg_chunk_time = int(all_time / done_chunks_count)



# Load, process and ingest data by small chunks from ChemBL without storing anything localy.

# P.S. I know that usually it's better to divide this to load_func and ingest_func, 
# but I decided that I don't want to store all this data on my laptop, so 
# I immediately pushe every data chunk to postgres DB on AWS.
# I also parallelize it for better performance. 

def handle_chembl_data() -> None:
    chembl_url = Variable.get('chembl_url')
    logging.info('Loading molecules from ChEMBL...')

    # first api call just to get page metadata
    # used to define for_loop insted of while_loop
    _, meta = _get_data_chunk(chembl_url, 1, 0)

    latest_chunk_num = 0 # _fetch_last_chunk_offset()
    chunk_size = 2000
    total_count = meta['total_count']

    # set up multiprocessing stuff
    num_of_workers = int(Variable.get('num_of_workers'))
    start_intervals = np.linspace(latest_chunk_num, total_count, num_of_workers, endpoint=False,  dtype=int)
    end_intervals = np.linspace(start_intervals[1], total_count, num_of_workers, endpoint=True, dtype=int)
    
    pool = multiprocessing.Pool(processes=num_of_workers)
    pool.map(_worker, [(start, end, chunk_size) for start, end in zip(start_intervals, end_intervals)])

    logging.info('Molecules from ChEMBL have been loaded and ingested')
