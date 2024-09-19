MODEL (
  name br_ms.territorio,
  kind SEED
);

 SELECT
    * 
FROM ST_READ('data/inputs/regioes_saude.xlsx') r
order by all;
 

