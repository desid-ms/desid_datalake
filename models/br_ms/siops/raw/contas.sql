MODEL (
  name raw.siops__contas,
  kind FULL
);
SELECT '2022' as competencia, 
 * FROM ST_READ('data/inputs/siops/contas_2022_corrigida.xlsx') order by all;