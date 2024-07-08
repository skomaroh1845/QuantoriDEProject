import logging

from typing import Union
from rdkit import RDLogger 

import rdkit
from rdkit.Chem import AllChem
from rdkit import Chem
import pandas as pd

from airflow.providers.postgres.hooks.postgres import PostgresHook




def _get_fingerprint(smile: str) -> Union[rdkit.DataStructs.cDataStructs.ExplicitBitVect, None]:
    try:
        mol = Chem.MolFromSmiles(smile)
    except:
        return None
    
    if mol is None:
        return None  # Return None if the SMILES string is invalid
    else:
        return AllChem.GetMorganFingerprintAsBitVect(mol, radius=2, nBits=1024).ToBase64()


# calc finger prints for molecules from passed table 
def calc_mols_fingerprints(source_table_name: str, dest_table_name: str, is_chembl_data: bool, ti) -> None:
    
    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    engine = postgres_hook.get_sqlalchemy_engine()
    connection = postgres_hook.get_conn()
    path_to_download = '/opt/airflow/data/'
    fp_files_names = []

    # by my estimations silver_chembl_id table with ~2.4*10^6 raws should be ~500 Mb
    # lets handle with it by chunks of ~10Mb size, than they should have ~50*10^3 raws
    # and loop will take 50 iterations 
    chunk_size = 50000

    with connection.cursor() as cursor:

        # get table size
        cursor.execute(
            f'select count(chembl_id) from {source_table_name};'
        )
        table_size = cursor.fetchall()[0][0]

        for offset in range(0, table_size, chunk_size):
            logging.info(f'Calculating fingerprints. Table size: {table_size}, curr offset: {offset}.')

            cursor.execute(
                f'select chembl_id, smile from {source_table_name} limit {chunk_size} offset {offset};'
            ) 
            # get data from DB
            tmp_df = pd.DataFrame(cursor.fetchall(), columns=['chembl_id', 'smile'])
            logging.info(f'Got {len(tmp_df)} mols from {source_table_name}.')

            # There are "" around smiles in silver_chembl_id table, no time to normal fix
            if is_chembl_data:
                tmp_df.smile = tmp_df.smile.apply(lambda x: x.strip('"'))
            
            # drop mols which don't have smiles 
            tmp_df = tmp_df.dropna()  
            logging.info(f'{len(tmp_df)} mols have smiles.')
            
            # calc fingerprints 
            RDLogger.DisableLog('rdApp.*')
            tmp_df['fingerprint'] = tmp_df.smile.apply(_get_fingerprint)
            RDLogger.EnableLog('rdApp.*')

            # drop mols which have invalid smiles (for them fingerprint is None)
            tmp_df = tmp_df.dropna() 
            logging.info(f'Fingerprints were successfully calculated for {len(tmp_df)} mols.')

            # load data to DB
            tmp_df = tmp_df.drop(columns=['smile'])
            tmp_df.to_sql(dest_table_name, engine, if_exists='append', index=False)

            # save to file for futher upload to s3. Only for mols from chembl 
            if is_chembl_data:
                tmp_df.to_csv(path_to_download + f'{dest_table_name}_offset_{offset}.csv', index=False)
                fp_files_names.append(path_to_download + f'{dest_table_name}_offset_{offset}.csv')
        
    if is_chembl_data:
        ti.xcom_push(key='fp_files_names', value=fp_files_names)




"""
 [18:49:40] SMILES Parse Error: Failed parsing SMILES '"O=C(O)c1csc(-n2ccc3ccc(Cl)cc32)n1"' for input: '"O=C(O)c1csc(-n2ccc3ccc(Cl)cc32)n1"'
airflow-scheduler-1  | [18:49:40] SMILES Parse Error: syntax error while parsing: "CCc1[nH]c(C(=O)OC)cc2c3ccccc3nc1-2"
airflow-scheduler-1  | [18:49:40] SMILES Parse Error: Failed parsing SMILES '"CCc1[nH]c(C(=O)OC)cc2c3ccccc3nc1-2"' for input: '"CCc1[nH]c(C(=O)OC)cc2c3ccccc3nc1-2"'
airflow-scheduler-1  | [1
"""
