

-- Choose 10 random source molecules and create a pivot view where first column is the
-- chembl_id of the target molecule, rest columns are the source molecule’s chembl_id and
-- each cell is the similarity score of the corresponding source and target molecules.

-- no permissions to create an extension with crosstab func(
-- CREATE EXTENSION IF NOT EXISTS tablefunc;
-- create or replace view similarity_of_10_random_mols as
-- select * from crosstab(
-- 'select target_molecule, source_molecule, similarity
-- from gold_fact_table_molecule_similarities
-- order by random()
-- limit 10'
-- );

-- this command works fine but show view only in console
-- select target_molecule, source_molecule, similarity from gold_fact_table_molecule_similarities order by random() limit 10 \crosstabview


-- In one query, display all source molecule chembl_id, target molecule chembl_id,
-- similarity score, chembl_id of the next most similar target molecule compared to the
-- target molecule of the current row, chembl_id of the second most similar
-- target_molecule to the current's row source_molecule.
create or replace view second_third_most_similar
as
select 
    target_molecule as "target molecule chembl_id", 
    source_molecule as "source molecule chembl_id",
    similarity as "similarity score",
    nth_value(source_molecule,2) 
        over(partition by target_molecule order by similarity desc rows between current row and unbounded following) 
        as "next most similar",
    nth_value(source_molecule,3) 
        over(partition by target_molecule order by similarity desc rows between current row and unbounded following) 
        as "second most similar"
from gold_fact_table_molecule_similarities s
join gold_dim_table_molecule_properties p on s.source_molecule = p.chembl_id;


-- Average similarity score per:
-- i. Source_molecule
-- ii. Source_molecule’s aromatic_rings
-- iii. Source_molecule’s heavy_atoms
-- iv. Whole dataset
-- For the last group replace nulls produced by aggregation with “TOTAL” string.
-- The view shouldn’t contain UNION/UNION ALL.

create or replace view avg_smlrt_per_aromatic_rings_and_heavy_atoms 
as 
select
    case when target_molecule is null then 'TOTAL' else target_molecule end as "target molecule",
    avg(similarity) as "avg similarity per source_molecule",
    case 
        when sum(aromatic_rings) = 0 then null 
        else sum(similarity) / sum(aromatic_rings)
    end as "avg similarity per source_molecule’s aromatic_rings",
    case 
        when sum(heavy_atoms) = 0 then null 
        else sum(similarity) / sum(heavy_atoms)
    end as "avg similarity per source_molecule’s heavy_atoms"
from gold_fact_table_molecule_similarities s
join gold_dim_table_molecule_properties p on s.source_molecule = p.chembl_id
group by grouping sets ((target_molecule), ());

