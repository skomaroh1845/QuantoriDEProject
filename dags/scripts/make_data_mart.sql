
create table if not exists gold_dim_table_molecule_properties(
    chembl_id text not null,
    molecule_type text,
    mw_freebase numeric,
    alogp numeric,
    psa numeric,
    cx_logp numeric,
    molecular_species text,
    full_mwt numeric,
    aromatic_rings int,
    heavy_atoms int,
    unique (chembl_id)
);


create or replace function move_molecules_to_data_mart()
returns varchar
language plpgsql
as $$
begin
    truncate table gold_dim_table_molecule_properties;

    insert into gold_dim_table_molecule_properties(
        chembl_id,molecule_type,mw_freebase,alogp,psa,cx_logp,molecular_species,full_mwt,aromatic_rings,heavy_atoms
    )
    select distinct
        s.source_molecule::text as "chembl_id",
        p.molecule_type::text as "molecule_type",
        nullif(trim(both '"' from p.mw_freebase), 'null')::numeric as "mw_freebase",
        nullif(trim(both '"' from p.alogp), 'null')::numeric as "alogp",
        nullif(trim(both '"' from p.psa), 'null')::numeric as "psa",
        nullif(trim(both '"' from p.cx_logp), 'null')::numeric as "cx_logp",
        trim(both '"' from p.molecular_species)::text as "molecular_species",
        nullif(trim(both '"' from p.full_mwt), 'null')::numeric as "full_mwt",
        p.aromatic_rings::int as "aromatic_rings",
        p.heavy_atoms::int as "heavy_atoms"
    from gold_fact_table_molecule_similarities s
    inner join silver_molecule_properties p on s.source_molecule = p.chembl_id;
    return 'done';
end; 
$$;

select move_molecules_to_data_mart();

