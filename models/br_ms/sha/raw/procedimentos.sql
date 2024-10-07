MODEL (
  name raw_sha.procedimentos,
  kind FULL
);

 SELECT * FROM ST_READ('data/inputs/tuss_sigtap_procedimentos.csv') order by all;