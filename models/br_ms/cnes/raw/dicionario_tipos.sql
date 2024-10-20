MODEL (
    name raw.cnes__tipos,
    kind FULL
);

SELECT * FROM read_json_auto('data/inputs/sha/cnes_tipos_basedosdados.json', 
);
