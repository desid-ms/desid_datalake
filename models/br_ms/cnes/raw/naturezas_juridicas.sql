MODEL (
    name raw.cnes__naturezas_juridicas,
    kind FULL
);

SELECT * FROM read_json_auto('data/inputs/sha/natureza_juridica_basedosdados.json' order by all
);
