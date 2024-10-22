MODEL (
    name siops.despesas,
    audits (
        saldo_positivo(blocking:=false),
    )
);
SELECT  competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, descricao_conta, fonte, destinacao, despesa_empenhada, despesa_liquidada, 
       (despesa_empenhada - despesa_liquidada) AS saldo_empenho,
       despesa_paga,
       (despesa_liquidada - despesa_paga) AS saldo_liquidado
FROM (
    SELECT competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, descricao_conta, fonte, destinacao, fase, valor_nominal
    
    FROM siops.lancamentos WHERE FASE IN ('Despesas Empenhadas',  'Despesas Liquidadas', 'Despesas Pagas')
) AS src
PIVOT (
   SUM(valor_nominal) FOR fase IN (
        'Despesas Empenhadas' AS despesa_empenhada,
        'Despesas Liquidadas' AS despesa_liquidada,
        'Despesas Pagas' AS despesa_paga
    )
) ORDER BY competencia, ibge, conta;

--  POST-STATEMENT
@IF(
  @runtime_stage = 'evaluating',
  COPY siops.despesas TO 'data/outputs/siops__despesas' 
    (FORMAT PARQUET, PARTITION_BY (competencia), OVERWRITE, FILENAME_PATTERN 'siops__despesas_{uuid}')
);


AUDIT (
    name saldo_positivo,
    dialect duckdb,
    blocking false
);
SELECT * FROM siops.despesas WHERE 
    conta <> 'ACDO000002'
    AND
    (saldo_empenho < 0
    OR saldo_liquidado < 0 );

