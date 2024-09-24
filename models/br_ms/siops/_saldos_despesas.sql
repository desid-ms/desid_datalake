MODEL (
    name siops.saldos_despesas,
    audits (
        saldo_positivo(blocking:=false),
    )
);
SELECT *,
       (Despesas_Empenhadas - Despesas_Liquidadas) AS Saldo_Empenho,
       (Despesas_Liquidadas - Despesas_Pagas) AS Saldo_Liquidado
FROM (
    SELECT ano, ente, codigo_ibge, codigo_conta, fonte_recursos, subfuncao, fase, valor_despesa
    FROM siops.despesas
) AS src
PIVOT (
    SUM(valor_despesa) FOR fase IN (
        'Despesas Empenhadas' AS Despesas_Empenhadas,
        'Despesas Liquidadas' AS Despesas_Liquidadas,
        'Despesas Pagas' AS Despesas_Pagas
    )
);


AUDIT (
    name saldo_positivo,
    dialect duckdb
);
SELECT * FROM siops.saldos_despesas WHERE 
    codigo_conta !='ACDO000002'
    AND
    (Saldo_Empenho < 0
    OR Saldo_Liquidado < 0 )