import logging
from itertools import repeat
import multiprocessing

import rdkit
from rdkit import DataStructs
from rdkit.DataStructs.cDataStructs import ExplicitBitVect
import pandas as pd
import psycopg2

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import (
    Variable,
)


def _get_table_chunk(
        cursor: psycopg2.extensions.cursor, 
        table_name: str, 
        chunk_size: int, 
        offset: int,
    ) -> pd.DataFrame:
    
    cursor.execute(
        f'select chembl_id, fingerprint from {table_name} limit {chunk_size} offset {offset};'
    )
    return pd.DataFrame(cursor.fetchall(), columns=[f'chembl_id', f'fingerprint'])


def _get_similarity(
        target_fp: ExplicitBitVect,
        fp_base64: str, 
    ) -> float:

    fingerprint = ExplicitBitVect(2048) 
    fingerprint.FromBase64(fp_base64)
    return DataStructs.TanimotoSimilarity(fingerprint, target_fp)


def _calc_similarity_for_chunk(
        target_fp: rdkit.DataStructs.cDataStructs.ExplicitBitVect,
        chunk_df: pd.DataFrame
    ) -> pd.DataFrame:

    # deep copy because df will be changed
    chunk_df_copy = chunk_df.copy(deep=True) 

    chunk_df_copy['similarity'] = chunk_df_copy.apply(
        lambda raw: _get_similarity(target_fp, raw.fingerprint)
        )

    # drop unnecessary data
    chunk_df_copy.drop(columns=['fingerprint'])
    
    return chunk_df_copy


def calc_similarity_scores(
        chembl_fingerprints_table: str, 
        target_fingerprints_table: str,
        ti
    ) -> None:
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    connection = postgres_hook.get_conn()
    num_of_workers = int(Variable.get('num_of_workers'))
    pool = multiprocessing.Pool(processes=num_of_workers)

    # by my estimations silver_chembl_fingerprints table with ~2.4*10^6 raws should be ~500 Mb
    # lets handle with it by chunks of ~10Mb size, than they should have ~50*10^3 raws
    # and loop will take 50 iterations.
    # table silver_target_fingerprints has only ~10^2 raws and will be loaded at ones 
    chunk_size = 50000

    with connection.cursor() as cursor:

        # get target fingerprints
        target_df = _get_table_chunk(cursor, target_fingerprints_table, chunk_size, 0)

        # get chembl table size
        cursor.execute(
            f'select count(chembl_id) from {chembl_fingerprints_table}'
        )
        table_size = cursor.fetchall()[0][0]
        
        # get all chembl fingerprints
        chebml_chunk_list = []
        for offset in range(0, table_size, chunk_size):
            tmp_chembl_df = _get_table_chunk(cursor, chembl_fingerprints_table, chunk_size, offset)
            chebml_chunk_list.append(tmp_chembl_df)

        paths = []
        for i, raw in target_df.iterrows():
            target_id = raw.chembl_id.values[0]

            target_fp = ExplicitBitVect(2048) 
            target_fp.FromBase64(raw.fingerprint.values[0])

            logging.info(f'Calculating Tanimoto similarity, done target molecules: {i}/{len(target_df)}.')
            
            scores_chunk_list = pool.starmap(
                _calc_similarity_for_chunk, 
                zip(repeat(target_fp), chebml_chunk_list),
            )
            all_scores_for_mol = pd.concat(scores_chunk_list)

            # save to parquet file for further uploading to s3 
            path = f'/opt/airflow/{target_id}_similarity_scores.parquet'
            all_scores_for_mol.to_parquet(path, index=False)
            paths.append(path)
        
        ti.xcom_push(key=f'parquet_paths', value=paths)


            
            