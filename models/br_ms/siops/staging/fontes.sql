MODEL (
  name staging.siops__fontes,
  kind FULL
);

 SELECT  
 * FROM ST_READ('data/inputs/siops/fontes.xlsx', layer = 'FONTES', open_options = ['HEADERS=FORCE']) order by all;