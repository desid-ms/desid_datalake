MODEL (
    name siops.despesas,
    -- audits (
    --     saldo_positivo(blocking:=false),
    -- )
);
SELECT  competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, ds_conta, fonte, destinacao, despesas_empenhadas, despesas_liquidadas, 
       (Despesas_Empenhadas - Despesas_Liquidadas) AS saldo_empenho,
       despesas_pagas,
       (Despesas_Liquidadas - Despesas_Pagas) AS saldo_liquidado
FROM (
    SELECT competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, ds_conta, fonte, destinacao, fase, valor_nominal
    
    FROM siops.lancamentos WHERE FASE IN ('Despesas Empenhadas',  'Despesas Liquidadas', 'Despesas Pagas')
) AS src
PIVOT (
   SUM(valor_nominal) FOR fase IN (
        'Despesas Empenhadas' AS despesas_empenhadas,
        'Despesas Liquidadas' AS despesas_liquidadas,
        'Despesas Pagas' AS despesas_pagas
    )
) ORDER BY competencia, ibge, conta;


-- AUDIT (
--     name saldo_positivo,
--     dialect duckdb
-- );
-- SELECT * FROM siops.saldos_despesas WHERE 
--     conta <> 'ACDO000002'
--     AND
--     (Saldo_Empenho < 0
--     OR Saldo_Liquidado < 0 )
