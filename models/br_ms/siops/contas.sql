MODEL (
  name siops.contas,
  kind FULL
);

 SELECT '2022'as competencia, 
 * FROM ST_READ('data/inputs/siops/contas_2022_corrigida.xlsx') order by all;

--  POST-STATEMENT
@IF(
  @runtime_stage = 'evaluating',
  COPY siops.receitas TO 'data/outputs/siops__contas' 
    (FORMAT PARQUET, PARTITION_BY (competencia), OVERWRITE, FILENAME_PATTERN 'siops__contas_{uuid}')
);
