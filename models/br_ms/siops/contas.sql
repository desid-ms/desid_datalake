MODEL (
  name siops.contas,
  kind FULL
);

 SELECT '2022'as competencia, 
 * FROM ST_READ('data/inputs/siops/contas_2022.xlsx') order by all;