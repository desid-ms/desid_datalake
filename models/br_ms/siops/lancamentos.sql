MODEL (
    name siops.lancamentos, 
    depends_on ["staging.siops__periodos", "staging.siops__homologados", "siconfi.entes"],
    kind FULL,
    audits(
        number_of_rows(threshold := 3000000),
        not_null(columns := (competencia, ibge, ente, capital, regiao, uf, esfera, fase, conta, descricao_conta, valor_nominal)),
        accepted_values(name:="periodos_validos", column := competencia, is_in=('2022-6')), -- períodos válidos
        accepted_range(name:="valores_validos_nos_lancamentos", column := valor_nominal, min_v := 0, max_v := 1000000000000, inclusive := false, blocking:=false),
    )
);

WITH
    -- TODOS_VALORES une tb_vl_valores de estados e municípios
    TODOS_VALORES AS (
        SELECT
            CO_MUNICIPIO AS CODIGO_IBGE,
            * EXCLUDE (CO_MUNICIPIO)
        FROM
            RAW.SIOPS__TB_VL_VALORES
        UNION ALL
        SELECT
            CO_UF AS CODIGO_IBGE,
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
            -- 3_1 3_2... 3_18, 4_1..4_18..., 95_1..95_18 são as pastas_hierarquias válidas
            WHERE (REGEXP_MATCHES(co_pasta_hierarquia, '^(3|4|6|7|8|9|10|86|87|88|89|90|94|95)_([1-9]|1[0-8])$') OR CO_PASTA = 1) 
            AND CO_TIPO < 23 -- Remove fases que são Totais Gerais
            AND CO_TIPO NOT IN (14, 20, 21) -- Remove Restos a Pagar e Totalizadoras de Receitas para base de cálculo ASPS
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
            AND H.IBGE_ENTE = V.CODIGO_IBGE
    )
 SELECT
     H.COMPETENCIA as competencia, -- mudar para periodo?  exercicio?
     H.CODIGO_IBGE as ibge,
     S.ENTE as ente,
     CASE WHEN S.CAPITAL = 1 THEN 'S' ELSE 'N' END AS capital,
     S.REGIAO as regiao,
     S.UF as uf,
     CASE
         WHEN S.ESFERA = 'D' THEN 'Distrital'
         WHEN S.ESFERA = 'M' THEN 'Municipal'
         WHEN S.ESFERA = 'E' THEN 'Estadual'
         WHEN S.ESFERA = 'U' THEN 'Federal'
     END AS esfera,
     S.POPULACAO as populacao,
     -- remove conteúdo entre parenteses e espaços em branco
     TRIM(REGEXP_REPLACE(REGEXP_REPLACE(C.NO_COLUNA, '\s*=.*$', ''), '\s*\([a-z]\)', '')) AS fase,
    -- C.NO_COLUNA as fase,
     FS.FONTE as fonte,
     FS.SUBFUNCAO AS destinacao,
     CT.CODIGO_CONTA AS conta,
     CT.DESCRICAO_CONTA AS descricao_conta,
     H.NU_VALOR AS valor_nominal
 FROM
     HOMOLOGADOS H 
     JOIN SIOPS.CONTAS CT ON CT.CODIGO_CONTA_SIOPS = H.CO_ITEM
     JOIN RAW.SIOPSUF__TB_PROJ_COLUNA C ON H.CO_TIPO = C.CO_SEQ_COLUNA
     LEFT JOIN STAGING.SIOPS__FONTES_SUBFUNCOES FS ON FS.PASTA_HIERARQUIA = H.CO_PASTA_HIERARQUIA
     JOIN SICONFI.ENTES S ON H.CODIGO_IBGE = S.codigo_ibge_6
 ORDER BY competencia, ibge, fase, conta, fonte, destinacao;


--  POST-STATEMENT
@IF(
  @runtime_stage = 'evaluating',
  COPY siops.lancamentos TO 'data/outputs/siops__lancamentos' 
    (FORMAT PARQUET, PARTITION_BY (competencia), OVERWRITE, FILENAME_PATTERN 'siops__lancamentos_{uuid}')
);