WITH atendimentos_rs AS (
   SELECT
       ID_ATENDIMENTO,
       MIN(CASE value
           WHEN '1' THEN 309050235  -- Medicina tradicional chinesa
           WHEN '2' THEN 101050097  -- Antroposofia aplicado a saúde
           WHEN '3' THEN 309050197  -- Homeopatia
           WHEN '4' THEN 309050200  -- Fitoterapia
           WHEN '5' THEN 309050227  -- Ayuverda
           ELSE 101050020  -- Outra
       END) AS CO_PROCEDIMENTO
   FROM raw.sisab__es_atendimentos
   WHERE key = 'RS'
   GROUP BY ID_ATENDIMENTO
),

atendimentos_pca AS (
   SELECT 
       ID_ATENDIMENTO,
       MIN(CASE LEFT(value, 6)
           WHEN 'ABP001' THEN 301010110  -- PRE_NATAL
           WHEN 'ABP002' THEN 301010129  -- PUERPERIO
           WHEN 'ABP004' THEN 301010277  -- PUERICULTURA
           WHEN 'ABP011' THEN 301010099  -- TABAGISMO/TUBERCULOSE
       END) AS CO_PROCEDIMENTO
   FROM raw.sisab__es_atendimentos
   WHERE key = 'PCA' 
   AND LEFT(value, 6) IN ('ABP001', 'ABP002', 'ABP004', 'ABP011')
   GROUP BY ID_ATENDIMENTO
),

main_query as ( 
    SELECT
       S.*,
       COALESCE(
           CASE 
               WHEN T1.valor = 'Domicílio' THEN 301010137
               WHEN T2.valor = 'Escuta inicial / Orientação' THEN 301040079
               WHEN T2.valor = 'Atendimento de urgência' THEN 301060037
           END,
           pca.CO_PROCEDIMENTO,
           rs.CO_PROCEDIMENTO,
           CASE 
               WHEN LEFT(S.CO_CBO_OCUPACAO, 4) in ('2231', '2233', '2251', '2252', '2253') THEN 301010064 -- Médico 
               ELSE 301010030 -- outras profissões
           END
       ) AS CO_PROCEDIMENTO
    FROM raw.sisab__es S
    LEFT JOIN tipos t1 ON t1.tabela = 'LOCAL_DE_ATENDIMENTO' AND t1.chave = S.CO_LOCAL_ATENDIMENTO
    LEFT JOIN tipos t2 ON t2.tabela = 'TIPO_ATENDIMENTO' AND t2.chave = S.CO_TIPO_ATENDIMENTO
    LEFT JOIN atendimentos_pca pca ON pca.ID_ATENDIMENTO = S.ID_ATENDIMENTO
    LEFT JOIN atendimentos_rs rs ON rs.ID_ATENDIMENTO = S.ID_ATENDIMENTO
    WHERE CO_TIPO_FICHA_ATENDIMENTO = 4 -- Ficha de Atendimento Individual
)

SELECT 
    m.*,
    p.*
FROM main_query m
LEFT JOIN sha.procedimentos p 
    ON m.co_procedimento = p.codigo_procedimento;
    