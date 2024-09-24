MODEL (
  name br_ms.territorio,
  kind FULL
);

 SELECT
    * 
FROM ST_READ('data/inputs/regioes_saude.xlsx') r
order by all;
 

