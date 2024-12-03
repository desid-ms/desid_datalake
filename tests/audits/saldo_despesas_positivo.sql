select *
from (
        SELECT ano,
            ente,
            ibge,
            conta,
            fonte,
            destinacao,
            SUM(
                CASE
                    WHEN fase = 'Receitas Orçadas' THEN valor_nominal
                    ELSE 0
                END
            ) AS Receitas_Orcadas,
            SUM(
                CASE
                    WHEN fase = 'Receitas Realizadas Brutas' THEN valor_nominal
                    ELSE 0
                END
            ) AS Receitas_Realizadas,
            SUM(
                CASE
                    WHEN fase = 'Despesas Empenhadas' THEN valor_nominal
                    ELSE 0
                END
            ) AS Despesas_Empenhadas,
            SUM(
                CASE
                    WHEN fase = 'Despesas Liquidadas' THEN valor_nominal
                    ELSE 0
                END
            ) AS Despesas_Liquidadas,
            SUM(
                CASE
                    WHEN fase = 'Despesas Pagas' THEN valor_nominal
                    ELSE 0
                END
            ) AS Despesas_Pagas,
            (Despesas_Empenhadas - Despesas_Liquidadas) AS Saldo_Empenho,
            (Despesas_Liquidadas - Despesas_Pagas) AS Saldo_Liquidado
        FROM dataset
        GROUP BY ano,
            ente,
            ibge,
            conta,
            fonte,
            destinacao
    )
WHERE 
    CONTA !='ACDO000002'
    AND
    (Saldo_Empenho < 0
    OR Saldo_Liquidado < 0 )

    