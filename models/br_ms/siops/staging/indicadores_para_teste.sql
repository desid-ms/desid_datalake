MODEL (
  name staging.siops__base_indicadores,
  kind FULL
);

 SELECT  
 * FROM ST_READ('data/inputs/siops/indicadores_auditoria_Base-2022-6_09_02_2022.xlsx',open_options = ['HEADERS=FORCE']) WHERE ANO is not NULL order by all;