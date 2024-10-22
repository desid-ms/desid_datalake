MODEL (
    name siops.receitas_deducoes,
    
);
    
SELECT  competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, descricao_conta, 
        receita_realizada_bruta,
        deducoes_transf, deducoes_fundeb, outras_deducoes
FROM (
    SELECT competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, descricao_conta, fase, valor_nominal
    FROM siops.lancamentos WHERE FASE IN ('Receitas Realizadas Brutas',  'Dedução de Transferências Const. e Legais a Municípios', 'Dedução Para Formação do FUNDEB', 'Deduções das Receitas')
) AS src
PIVOT (
   SUM(valor_nominal) FOR fase IN (
      'Dedução de Transferências Const. e Legais a Municípios' AS deducoes_transf,
      'Dedução Para Formação do FUNDEB' AS deducoes_fundeb,
      'Deduções das Receitas' AS outras_deducoes,
      'Receitas Realizadas Brutas' AS receita_realizada_bruta
    )
) ORDER BY competencia, ibge, conta;
