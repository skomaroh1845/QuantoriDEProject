
from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.trigger_rule import TriggerRule


from scripts.utils import empty_func
from scripts.check_chembl_data_existance import check_chembl_data_existance
from scripts.chembl_data_handling import handle_chembl_data
from scripts.check_for_new_input_data import check_for_new_input_data
from scripts.load_and_ingest_s3_input_data import (
    load_s3_input_data, 
    ingest_s3_input_data,
)
from scripts.calc_mols_fingerprints import calc_mols_fingerprints
from scripts.calc_similarity_scores import calc_similarity_scores
from scripts.upload_mols_data_to_s3 import upload_mols_data


with DAG(
    dag_id='similar_mols_searching_dag',
    start_date=pendulum.datetime(year=2024, month=7, day=5),
    schedule=None,
    tags=['similar_mols'],
    description='A DAG to searching top 10 similar molecules from ChEMBL for set of molecules from S3 bucket',
    catchup=False,
) as dag:
    
    start_op = EmptyOperator(task_id='start')

    check_chembl_data_existance_op = BranchPythonOperator(
        task_id='check_chembl_data_existance', 
        python_callable=check_chembl_data_existance,
    )

    handle_chembl_data_op = PythonOperator(
        task_id = 'handle_chembl_data',
        python_callable = handle_chembl_data,
    )

    check_for_new_input_data_op = BranchPythonOperator(
        task_id='check_for_new_input_data', 
        python_callable=check_for_new_input_data
        ) 
    
    # creates tables if they are not exist
    create_silver_layer_tables_op = PostgresOperator(
        task_id='create_silver_layer_tables',
        sql='scripts/create_silver_tables.sql',
        postgres_conn_id='postgres_AWS'
    )

    # move chembl data from bronze to silver layer
    move_chembl_to_silver_op = PostgresOperator(
        task_id='move_chembl_to_silver',
        sql='scripts/move_chembl_to_silver.sql',
        postgres_conn_id='postgres_AWS'
    )

    load_s3_input_data_op = PythonOperator(
        task_id='load_s3_input_data', 
        python_callable=load_s3_input_data
        )

    ingest_s3_input_data_op = PythonOperator(
        task_id='ingest_s3_input_data', 
        python_callable=ingest_s3_input_data
        )

    check_1_op = EmptyOperator(
        task_id='check_1', 
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
    
    calc_chembl_mols_fingerprints_op = PythonOperator(
        task_id='calc_chembl_mols_fingerprints', 
        python_callable=calc_mols_fingerprints,
        op_kwargs={
            'source_table_name': 'silver_chembl_id',
            'dest_table_name': 'silver_chembl_fingerprints',
            'is_chembl_data' : True,
        }
        )
    
    upload_chembl_mols_fingerprints_op = PythonOperator(  
        task_id='upload_chembl_mols_fingerprints', 
        python_callable=upload_mols_data,  # to S3 bucket
        op_kwargs={
            'folder_name': 'chembl_mols_fingerprints/', # should end with /
            'xcom_pull_key': 'fp_files_names',
            'xcom_pull_task_id': 'calc_chembl_mols_fingerprints',
            'del_after': False,
        }
        )  
    
    calc_target_mols_fingerprints_op = PythonOperator(
        task_id='calc_target_mols_fingerprints', 
        python_callable=calc_mols_fingerprints,
        op_kwargs={
            'source_table_name': 'silver_target_molecules',
            'dest_table_name': 'silver_target_fingerprints',
            'is_chembl_data' : False,
        }
        )

    calc_similarity_scores_op = PythonOperator(
        task_id='calc_similarity_scores', 
        python_callable=calc_similarity_scores,
        op_kwargs={
            'chembl_fingerprints_table': 'silver_chembl_fingerprints', 
            'target_fingerprints_table': 'silver_target_fingerprints',
        }
        )

    upload_mols_similarities_op = PythonOperator(
        task_id='upload_mols_similarities', 
        python_callable=empty_func,#upload_mols_data,  # to S3 bucket
        op_kwargs={
            'folder_name': 'similarity_scores/', # should end with /
            'xcom_pull_key': 'parquet_paths',
            'xcom_pull_task_id': 'calc_similarity_scores',
            'del_after': False,
        }
        )

    take_top10_most_similar_mols_op = PythonOperator(task_id='take_top_10_most_similar_mols', python_callable=empty_func)

    make_data_mart_op = PythonOperator(task_id='make_data_mart', python_callable=empty_func)

    make_db_views_for_top10_mols_op = PythonOperator(task_id='make_db_views_for_top10_mols', python_callable=empty_func)

    make_db_views_for_all_mols_op = PythonOperator(task_id='make_db_views_for_all_mols', python_callable=empty_func)

    check_2_op = EmptyOperator(
        task_id='check_2', 
        trigger_rule=TriggerRule.ALL_SUCCESS
        )

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )


    start_op >> check_chembl_data_existance_op >> check_for_new_input_data_op >> finish_op
    start_op >> check_chembl_data_existance_op >> check_for_new_input_data_op \
             >> load_s3_input_data_op >> ingest_s3_input_data_op >> check_1_op
    start_op >> check_chembl_data_existance_op >> handle_chembl_data_op\
             >> create_silver_layer_tables_op >> move_chembl_to_silver_op >> check_1_op

    check_1_op >> [calc_chembl_mols_fingerprints_op, calc_target_mols_fingerprints_op]\
               >> calc_similarity_scores_op\
               >> [upload_chembl_mols_fingerprints_op, 
                   upload_mols_similarities_op, 
                   take_top10_most_similar_mols_op]
    
    upload_chembl_mols_fingerprints_op >> check_2_op
    upload_mols_similarities_op >> check_2_op

    take_top10_most_similar_mols_op >> make_data_mart_op\
               >> [make_db_views_for_top10_mols_op, make_db_views_for_all_mols_op]\
               >> check_2_op >> finish_op

