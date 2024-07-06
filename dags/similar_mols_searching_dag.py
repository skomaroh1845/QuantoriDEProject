
from airflow import DAG
import pendulum
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.utils.trigger_rule import TriggerRule


from scripts.utils import empty_func
from scripts.check_chembl_data_existance import check_chembl_data_existance
from scripts.chembl_data_handling import handle_chembl_data
from scripts.load_s3_input_data import load_s3_input_data
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

    # handle_chembl_data_op = PythonOperator(
    #     task_id = 'handle_chembl_data',
    #     python_callable = handle_chembl_data,
    # )

    # check_for_new_input_data_op = BranchPythonOperator(task_id='check_for_new_input_data', python_callable=empty_func)

    # load_chembl_data_op = PythonOperator(task_id='load_chembl_data', python_callable=empty_func)    

    load_s3_input_data_op = PythonOperator(task_id='load_s3_input_data', python_callable=load_s3_input_data)

    upload_mols_data_op = PythonOperator(task_id='upload_mols_data', python_callable=upload_mols_data)

    # ingest_s3_input_data_op = PythonOperator(task_id='ingest_s3_input_data', python_callable=empty_func)

    # ingest_chembl_data_op = PythonOperator(task_id='ingest_chembl_data', python_callable=empty_func)

    # check_1_op = EmptyOperator(task_id='check_1')
    
    # calc_mols_fingerprints_op = PythonOperator(task_id='calc_mols_fingerprints', python_callable=empty_func)

    # upload_mols_fingerprints_op = PythonOperator(task_id='upload_mols_fingerprints', python_callable=empty_func)  # to S3 bucket

    # calc_similarity_scores_op = PythonOperator(task_id='calc_similarity_scores', python_callable=empty_func)

    # upload_mols_similarities_op = PythonOperator(task_id='upload_mols_similarities', python_callable=empty_func)

    # check_2_op = EmptyOperator(task_id='check_2')

    # take_top10_most_similar_mols_op = PythonOperator(task_id='take_top_10_most_similar_mols', python_callable=empty_func)

    # make_data_mart_op = PythonOperator(task_id='make_data_mart', python_callable=empty_func)

    # make_db_views_for_top10_mols_op = PythonOperator(task_id='make_db_views_for_top10_mols', python_callable=empty_func)

    # make_db_views_for_all_mols_op = PythonOperator(task_id='make_db_views_for_all_mols', python_callable=empty_func)

    finish_op = EmptyOperator(
        task_id='finish',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # start_op >> chech_chembl_data_existance_op >> check_for_new_input_data_op >> finish_op
    # start_op >> chech_chembl_data_existance_op >> check_for_new_input_data_op >> load_s3_input_data_op >> ingest_s3_input_data_op >> check_1_op
    # start_op >> chech_chembl_data_existance_op >> load_chembl_data_op >> ingest_chembl_data_op >> check_1_op

    # check_1_op >> calc_mols_fingerprints_op >> calc_similarity_scores_op

    # calc_similarity_scores_op >> [upload_mols_fingerprints_op, upload_mols_similarities_op] >> check_2_op
    # calc_similarity_scores_op >> take_top10_most_similar_mols_op >> make_data_mart_op\
    #                           >> [make_db_views_for_top10_mols_op, make_db_views_for_all_mols_op] >> check_2_op
    
    # check_2_op >> finish_op

    # start_op >> check_chembl_data_existance_op >> handle_chembl_data_op >> load_s3_input_data_op >> upload_mols_data_op >> finish_op
    start_op >> check_chembl_data_existance_op >> load_s3_input_data_op >> upload_mols_data_op >> finish_op

    



