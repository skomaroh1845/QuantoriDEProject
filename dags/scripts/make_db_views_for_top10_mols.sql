


create or replace view avg_similarity_score_per_source_molecule 
as 
select target_molecule, avg(similarity) as avg_similarity_score_per_source_molecule
from gold_fact_table_molecule_similarities
group by target_molecule;



create or replace view avg_alogp_deviation_of_similar_molecule
as 
select target_molecule, stddev(alogp) as avg_alogp_deviation
from gold_fact_table_molecule_similarities s
join gold_dim_table_molecule_properties p on s.source_molecule = p.chembl_id
group by target_molecule;