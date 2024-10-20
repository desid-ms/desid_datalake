MODEL (
    name siops.lancamentos, 
    depends_on ["staging.siops__periodos"],
    kind FULL);

WITH
    -- TODOS_VALORES une tb_vl_valores de estados e municípios
    TODOS_VALORES AS (
        SELECT
            CO_MUNICIPIO AS IBGE_ENTE,
            * EXCLUDE (CO_MUNICIPIO)
        FROM
            RAW.SIOPS__TB_VL_VALORES
        UNION ALL
        SELECT
            CO_UF AS IBGE_ENTE,
            * EXCLUDE (CO_UF)
        FROM
            RAW.SIOPSUF__TB_VL_VALORES
    ),
    -- apenas valores de contas operacionais
    OPERACIONAIS AS (
        SELECT * FROM TODOS_VALORES WHERE CO_ITEM in 
        (
            SELECT CODIGO_CONTA_SIOPS::TEXT FROM SIOPS.CONTAS WHERE TIPO_CONTA = 'operacional'
        )
    ),
    -- apenas valores de receitas e despesas
    VALORES_RECEITAS_DESPESAS AS (
        SELECT * FROM OPERACIONAIS 
            WHERE (REGEXP_MATCHES(co_pasta_hierarquia, '^(3|4|6|7|8|9|10|86|87|88|89|90|94|95)_([1-9]|1[0-8])$') OR CO_PASTA = 1) 
            AND CO_TIPO < 23 -- Remove fases que são Totais Gerais
    ), 
    
    -- apenas valores que já foram homologados
    HOMOLOGADOS AS (
        SELECT
            P.PERIODO AS COMPETENCIA,
            V.*
        FROM
            VALORES_RECEITAS_DESPESAS V
            JOIN STAGING.SIOPS__PERIODOS P ON P.ANO = V.NU_ANO
            AND P.CODIGO_BIMESTRE_SIOPS = V.NU_PERIODO
            JOIN STAGING.SIOPS__HOMOLOGADOS H ON H.PERIODO = P.PERIODO
            AND H.IBGE_ENTE = V.IBGE_ENTE
    )
 SELECT
     H.COMPETENCIA,
     H.IBGE_ENTE,
     S.ENTE,
     CASE WHEN S.CAPITAL = 1 THEN 'S' ELSE 'N' END AS CAPITAL,
     S.REGIAO,
     S.UF,
     CASE
         WHEN S.ESFERA = 'D' THEN 'Distrital'
         WHEN S.ESFERA = 'M' THEN 'Municipal'
         WHEN S.ESFERA = 'E' THEN 'Estadual'
         WHEN S.ESFERA = 'U' THEN 'Federal'
     END AS ESFERA,
     S.POPULACAO,
     -- remove conteúdo entre parenteses e espaços em branco
     TRIM(REGEXP_REPLACE(REGEXP_REPLACE(C.NO_COLUNA, '\s*=.*$', ''), '\s*\([a-z]\)', '')) AS FASE,
     
     FS.FONTE,
     FS.SUBFUNCAO AS DESTINACAO,
     CT.CODIGO_CONTA,
     CT.DESCRICAO_CONTA AS CONTA,
     H.NU_VALOR AS VALOR_NOMINAL
 FROM
     HOMOLOGADOS H 
     JOIN SIOPS.CONTAS CT ON CT.CODIGO_CONTA_SIOPS = H.CO_ITEM
     JOIN RAW.SIOPSUF__TB_PROJ_COLUNA C ON H.CO_TIPO = C.CO_SEQ_COLUNA
     LEFT JOIN STAGING.SIOPS__FONTES_SUBFUNCOES FS ON FS.PASTA_HIERARQUIA = H.CO_PASTA_HIERARQUIA
     JOIN SICONFI.ENTES S ON H.IBGE_ENTE = S.codigo_ibge_6
 ORDER BY H.COMPETENCIA, IBGE_ENTE, FASE, CONTA, FONTE, SUBFUNCAO