MODEL (
  name staging.siops__subfuncoes,
  kind FULL
);

 SELECT  
 * FROM ST_READ('data/inputs/siops/subfuncoes.xlsx', layer = 'SUBFUNCOES', open_options = ['HEADERS=FORCE']) order by all;