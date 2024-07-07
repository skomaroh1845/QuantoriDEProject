

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook



def take_top_10_most_similar_mols(xcom_pull_key: str, xcom_pull_task_id: str, ti) -> None:

    files_names = ti.xcom_pull(task_ids=xcom_pull_task_id, key=xcom_pull_key)

    top_10_df_list = []

    for file in files_names:
        # get data
        target_id = file.split('/')[-1].split('_')[0]
        similarity_scores = pd.read_parquet(file)

        # get top 10 values
        sorted_similarity = similarity_scores.sort_values('similarity', ascending=False)
        top_10_scores = sorted_similarity.head(10)
        
        top_10_scores.columns = ['source_molecule', 'similarity']
        top_10_scores['target_molecule'] = target_id
        
        # if 11th largest score = 10th largest score 
        # than we have duplicates_of_last_largest_score
        if sorted_similarity.similarity.values[9]\
            == sorted_similarity.similarity.values[10]:
            
            top_10_scores['has_duplicates_of_last_largest_score'] = True

        top_10_df_list.append(top_10_scores)

    common_df = pd.concat(top_10_df_list)

    postgres_hook = PostgresHook(postgres_conn_id='postgres_AWS')
    engine = postgres_hook.get_sqlalchemy_engine()
    common_df.to_sql('gold_fact_table_molecule_similarities', engine, if_exists='append')




