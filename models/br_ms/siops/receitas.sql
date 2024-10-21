MODEL (
    name siops.receitas,
);
    
    SELECT 
    competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, ds_conta, 
    valor_nominal AS receitas_realizadas
    FROM siops.lancamentos
    WHERE FASE = 'Receitas Realizadas Brutas'
    ORDER BY competencia, ibge, conta