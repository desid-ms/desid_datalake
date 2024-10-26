MODEL (
  name raw.sha__tpups_hps,
  kind FULL
);

 SELECT * FROM ST_READ('data/inputs/sha/tp_ups_hp_code.csv') order by all;