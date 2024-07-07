create or replace function nkomarov.bronze_move_molecules_properties()
returns varchar
language plpgsql
as $$
begin
    truncate table nkomarov.silver_molecule_properties;
    insert into nkomarov.silver_molecule_properties(
        chembl_id,molecule_type,mw_freebase,alogp,psa,cx_logp,molecular_species,full_mwt,aromatic_rings,heavy_atoms
    )
    select distinct on
        (molecule_chembl_id) "molecule_chembl_id" as "chembl_id",
        molecule_type as "molecule_type",
        (molecule_properties::json->'mw_freebase')::varchar as "mw_freebase",
        (molecule_properties::json->'alogp')::varchar as "alogp",
        (molecule_properties::json->'psa')::varchar as "psa",
        (molecule_properties::json->'cx_logp')::varchar as "cx_logp",
        (molecule_properties::json->'molecular_species')::varchar as "molecular_species",
        (molecule_properties::json->'full_mwt')::varchar as "full_mwt",
        nullif((molecule_properties::json->'aromatic_rings')::varchar, 'null')::int as "aromatic_rings",
        nullif((molecule_properties::json->'heavy_atoms')::varchar, 'null')::int as "heavy_atoms"
    from nkomarov.bronze_raw_mols_data
    where (molecule_structures::json->'canonical_smiles')::varchar <> '';
    return 'done';
end; 
$$;

create or replace function nkomarov.bronze_move_molecules_id()
returns varchar
language plpgsql
as $$
begin
    truncate table nkomarov.silver_chembl_id;
    insert into nkomarov.silver_chembl_id(chembl_id, smile)
    select distinct on
        (molecule_chembl_id) "molecule_chembl_id" as "chembl_id",
        (molecule_structures::json->'canonical_smiles')::varchar as "smile"
    from nkomarov.bronze_raw_mols_data
    where (molecule_structures::json->'canonical_smiles')::varchar <> '';
    return 'done';
end; 
$$;


select bronze_move_molecules_properties();
select bronze_move_molecules_id();
