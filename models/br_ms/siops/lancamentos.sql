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
    ) ,
    -- apenas valores de receitas e despesas
    VALORES_RECEITAS_DESPESAS AS (
        SELECT
            *
        FROM
            OPERACIONAIS
        WHERE
            CO_PASTA_HIERARQUIA IN (
                SELECT
                    '2' AS pasta_hierarquia -- RECEITAS
                UNION
                SELECT DISTINCT
                    pasta_hierarquia -- DESPESAS são pasta_hierarquia com fonte e subfuncao
                FROM
                    staging.siops__fontes_subfuncoes
            )
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
    H.COMPETENCIA, -- ANO-BIMESTRE do lançamento
    H.IBGE_ENTE, -- Código IBGE de 6 dígitos referente ao ente federado
    S.ENTE, -- Nome do Ente federado
    S.CAPITAL, -- 1 se é capital de estado ou 0 se não é capital
    S.REGIAO, -- Região do País do Ente Federado (NO, SU, CO, NE, SE)
    S.UF, -- Unidade Federativa do ENTE ('BR' para estados e distrito federal)
    S.ESFERA, -- D = Distrito, E = Estado, M = Município, U = União
    S.POPULACAO, -- De 2024, segundo o SICONFI
    C.NO_COLUNA AS FASE, -- Fase orçamentária
    FS.FONTE, -- Fonte de recursos
    FS.SUBFUNCAO AS DESTINACAO, -- Destinação de recursos (Função Saúde/Subfunção)
    CT.CODIGO_CONTA AS CONTA, -- Item de Conta orçamentário do lançamento
    CT.DESCRICAO_CONTA, -- Descrição da Conta Orçamentária
    H.NU_VALOR AS VALOR_NOMINAL -- Valor nominal do lançamento
FROM
    HOMOLOGADOS H
    LEFT JOIN SIOPS.CONTAS CT ON CT.CODIGO_CONTA_SIOPS = H.CO_ITEM
    LEFT JOIN RAW.SIOPSUF__TB_PROJ_COLUNA C ON H.CO_TIPO = C.CO_SEQ_COLUNA
    LEFT JOIN STAGING.SIOPS__FONTES_SUBFUNCOES FS ON FS.PASTA_HIERARQUIA = H.CO_PASTA_HIERARQUIA
    LEFT JOIN SICONFI.ENTES S ON H.IBGE_ENTE = S.codigo_ibge_6
    ORDER BY H.COMPETENCIA, IBGE_ENTE, FASE, CONTA, FONTE, SUBFUNCAO