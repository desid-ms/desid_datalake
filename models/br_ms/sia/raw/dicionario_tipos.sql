MODEL (
    name raw.sia__tipos,
    kind FULL
);

SELECT * FROM read_json_auto('data/inputs/sha/sia_tipos_basedosdados.json', 
);
