MODEL (
  name staging.siops__fontes_subfuncoes,
  kind FULL
);

WITH 
    FONTES AS (
        SELECT DISTINCT codigo_fonte, fonte
        FROM staging.siops__fontes
    ),
    SUBFUNCOES AS (
        SELECT DISTINCT codigo_subfuncao, subfuncao
        FROM staging.siops__subfuncoes
    )
SELECT 
    F.codigo_fonte,
    F.fonte,
    S.codigo_subfuncao,
    S.subfuncao,
    F.codigo_fonte || '_' || S.codigo_subfuncao AS pasta_hierarquia
FROM 
    FONTES F
CROSS JOIN 
    SUBFUNCOES S
ORDER BY 
    F.codigo_fonte, 
    S.codigo_subfuncao;