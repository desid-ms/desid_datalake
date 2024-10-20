MODEL (
    name siops.saldos_despesas,
    -- audits (
    --     saldo_positivo(blocking:=false),
    -- )
);
SELECT *,
       (Despesas_Empenhadas - Despesas_Liquidadas) AS Saldo_Empenho,
       (Despesas_Liquidadas - Despesas_Pagas) AS Saldo_Liquidado
FROM (
    SELECT competencia, ente, ibge_ente, conta, fonte, destinacao, fase, valor_nominal
    FROM siops.lancamentos
) AS src
PIVOT (
   SUM(valor_nominal) FOR fase IN (
        'Despesas Empenhadas' AS Despesas_Empenhadas,
        'Despesas Liquidadas' AS Despesas_Liquidadas,
        'Despesas Pagas' AS Despesas_Pagas
    )
) ORDER BY competencia, ibge_ente, conta;


-- AUDIT (
--     name saldo_positivo,
--     dialect duckdb
-- );
-- SELECT * FROM siops.saldos_despesas WHERE 
--     conta <> 'ACDO000002'
--     AND
--     (Saldo_Empenho < 0
--     OR Saldo_Liquidado < 0 )
