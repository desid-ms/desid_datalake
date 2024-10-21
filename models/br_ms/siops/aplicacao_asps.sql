MODEL (
    name siops.aplicacao_asps,
    depends_on ["siops.lancamentos"],
);
SELECT competencia, ibge, ente, capital, regiao, uf, esfera, populacao, receitas_realizadas, despesas_empenhadas,  
       CAST((despesas_empenhadas / receitas_realizadas)*100 AS DECIMAL(18,2)) AS percentual_aplicacao
FROM (
    SELECT COMPETENCIA, IBGE,  ENTE, CAPITAL, REGIAO, UF, ESFERA, POPULACAO, fase, valor_nominal
    FROM siops.lancamentos WHERE FASE IN ('Receitas Realizadas da base para cálculo do percentual de aplicacao em ASPS', 'Despesas Empenhadas')
    GROUP BY ALL
) AS src
PIVOT (
   SUM(valor_nominal) FOR fase IN (
        'Receitas Realizadas da base para cálculo do percentual de aplicacao em ASPS' AS receitas_realizadas,
        'Despesas Empenhadas' AS despesas_empenhadas
    )
) ORDER BY competencia, ibge;


