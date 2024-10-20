-- Classificação Brasileira de Ocupações
MODEL (
  name br_mte.cbo,
  kind FULL
);

 SELECT * FROM read_json_auto('data/inputs/mte/cbo_2002_basedosdados.json') order by all;