MODEL (
    name siops.aplicacao_asps,
);
SELECT *, 
       CAST((Despesas_Pagas / Receitas_Realizadas)*100 AS DECIMAL(18,2)) AS Percentual_Aplicacao
FROM (
    SELECT COMPETENCIA, IBGE_ENTE,  ENTE, CAPITAL, REGIAO, UF, ESFERA, POPULACAO, fase, valor_nominal
    FROM siops.lancamentos WHERE FASE IN ('Receitas Realizadas da base para cálculo do percentual de aplicacao em ASPS', 'Despesas Pagas')
    GROUP BY ALL
) AS src
PIVOT (
   SUM(valor_nominal) FOR fase IN (
        'Receitas Realizadas da base para cálculo do percentual de aplicacao em ASPS' AS Receitas_Realizadas,
        'Despesas Pagas' AS Despesas_Pagas
    )
) ORDER BY competencia, ibge_ente;


