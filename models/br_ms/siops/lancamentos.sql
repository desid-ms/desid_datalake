MODEL (
  name siops.lancamentos,
  depends_on ["staging.siops__periodos", "staging.siops__homologados", "siconfi.entes"],
  kind FULL,
  audits (
    NUMBER_OF_ROWS(threshold := 3000000),
    NOT_NULL(
      columns := (
        competencia,
        ibge,
        ente,
        capital,
        regiao,
        uf,
        esfera,
        fase,
        conta,
        descricao_conta,
        valor_nominal
      )
    ),
    ACCEPTED_VALUES(name := "periodos_validos", "column" := competencia, is_in = (
      '2022-6'
    )), /* períodos válidos */
    ACCEPTED_RANGE(
      name := "valores_validos_nos_lancamentos",
      "column" := valor_nominal,
      min_v := 0,
      max_v := 1000000000000,
      inclusive := FALSE,
      blocking := FALSE
    )
  )
);

WITH TODOS_VALORES /* TODOS_VALORES une tb_vl_valores de estados e municípios */ AS (
  SELECT
    CO_MUNICIPIO AS CODIGO_IBGE,
    *
    EXCLUDE (CO_MUNICIPIO)
  FROM RAW.SIOPS__TB_VL_VALORES
  UNION ALL
  SELECT
    CO_UF AS CODIGO_IBGE,
    *
    EXCLUDE (CO_UF)
  FROM RAW.SIOPSUF__TB_VL_VALORES
), OPERACIONAIS /* apenas valores de contas operacionais */ AS (
  SELECT
    *
  FROM TODOS_VALORES
  WHERE
    CO_ITEM IN (
      SELECT
        CAST(CODIGO_CONTA_SIOPS AS TEXT)
      FROM SIOPS.CONTAS
      WHERE
        TIPO_CONTA = 'operacional'
    )
), VALORES_RECEITAS_DESPESAS /* apenas valores de receitas e despesas */ AS (
  SELECT
    *
  FROM OPERACIONAIS
  /* 3_1 3_2... 3_18, 4_1..4_18..., 95_1..95_18 são as pastas_hierarquias válidas */
  WHERE
    (
      REGEXP_MATCHES(co_pasta_hierarquia, '^(3|4|6|7|8|9|10|86|87|88|89|90|94|95)_([1-9]|1[0-8])$')
      OR CO_PASTA = 1
    )
    AND CO_TIPO < 23 /* Remove fases que são Totais Gerais */
    AND NOT CO_TIPO IN (14, 20, 21) /* Remove Restos a Pagar e Totalizadoras de Receitas para base de cálculo ASPS */
), HOMOLOGADOS /* apenas valores que já foram homologados */ AS (
  SELECT
    P.PERIODO AS COMPETENCIA,
    V.*
  FROM VALORES_RECEITAS_DESPESAS AS V
  JOIN STAGING.SIOPS__PERIODOS AS P
    ON P.ANO = V.NU_ANO AND P.CODIGO_BIMESTRE_SIOPS = V.NU_PERIODO
  JOIN STAGING.SIOPS__HOMOLOGADOS AS H
    ON H.PERIODO = P.PERIODO AND H.IBGE_ENTE = V.CODIGO_IBGE
)
SELECT
  H.COMPETENCIA AS competencia, /* periodo ao qual o lancamento se refere YYYY-B onde YYYY é ano e B é Bimestre */
  H.CODIGO_IBGE AS ibge, /* código IBGE do ente de 6 dífitos */
  S.ENTE AS ente, /* nome do ente federado que registrou o lançamento */
  CASE WHEN S.CAPITAL = 1 THEN 'S' ELSE 'N' END AS capital, /* se o ente é capital, 'S', ou não, 'N' */
  S.REGIAO AS regiao, /* região do país */
  S.UF AS uf, /* unidade federativa. Estados e DF estão na unidade federativa da união, BR */
  CASE
    WHEN S.ESFERA = 'D'
    THEN 'Distrital'
    WHEN S.ESFERA = 'M'
    THEN 'Municipal'
    WHEN S.ESFERA = 'E'
    THEN 'Estadual'
    WHEN S.ESFERA = 'U'
    THEN 'Federal'
  END AS esfera, /* esfera federativa */
  S.POPULACAO AS populacao, /* população 2022 (segundo SICONFI) */
  TRIM(REGEXP_REPLACE(REGEXP_REPLACE(C.NO_COLUNA, '\s*=.*$', ''), '\s*\([a-z]\)', '')) AS fase, /* fase orçamentária */
  FS.FONTE AS fonte, /* fonte de recursos dos lançamentos de despesa */
  FS.SUBFUNCAO AS destinacao, /* destinação dos recursos dos lançamentos de despesa(aka subfunção) */
  CT.CODIGO_CONTA AS conta, /* código da conta contábil do lançamento */
  CT.DESCRICAO_CONTA AS descricao_conta, /* descrição da conta contábil no plano de contas do SIOPS */
  H.NU_VALOR AS valor_nominal /* valor do lançamento no valor nominal corrente do período de competência */
FROM HOMOLOGADOS AS H
JOIN SIOPS.CONTAS AS CT
  ON CT.CODIGO_CONTA_SIOPS = H.CO_ITEM
JOIN RAW.SIOPSUF__TB_PROJ_COLUNA AS C
  ON H.CO_TIPO = C.CO_SEQ_COLUNA
LEFT JOIN STAGING.SIOPS__FONTES_SUBFUNCOES AS FS
  ON FS.PASTA_HIERARQUIA = H.CO_PASTA_HIERARQUIA
JOIN SICONFI.ENTES AS S
  ON H.CODIGO_IBGE = S.codigo_ibge_6
ORDER BY
  competencia,
  ibge,
  fase,
  conta,
  fonte,
  destinacao;

@IF(
  @runtime_stage = 'evaluating',
  COPY siops.lancamentos
  TO 'data/outputs/siops__lancamentos' WITH (
    FORMAT PARQUET,
    PARTITION_BY (
      competencia
    ),
    OVERWRITE_OR_IGNORE
  )
) /*  POST-STATEMENT */