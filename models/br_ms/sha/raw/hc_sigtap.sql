MODEL (
  name raw.sha__HC_SIGTAP,
  kind FULL
);

SELECT * FROM ST_READ('data/inputs/sha/HC_SIGTAP.xlsx') order by all;
-- SELECT * FROM ST_READ('data/inputs/sha/procedimentos_sigtap-raulino.xlsx') order by all;