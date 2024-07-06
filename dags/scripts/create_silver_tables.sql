create table if not exists nkomarov.silver_molecule_properties(
    chembl_id varchar not null,
    molecule_type varchar,
    mw_freebase varchar,
    alogp varchar,
    psa varchar,
    cx_logp varchar,
    molecular_species varchar,
    full_mwt varchar,
    aromatic_rings int,
    heavy_atoms int,
    unique (chembl_id)
);

create table if not exists nkomarov.silver_chembl_id(
    chembl_id varchar not null,  -- molecule id 
    smile varchar,
    foreign key (chembl_id) references nkomarov.silver_molecule_properties (chembl_id),
    unique (chembl_id)
);