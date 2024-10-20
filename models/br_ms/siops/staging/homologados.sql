 -- ENTE, ANO, PERÍODO para os quais os dados na tb_vl_valores estão homologados.
 MODEL (
  name staging.siops__homologados,
  depends_on ["RAW.SIOPS__TH_AUT_TRANSM", "RAW.SIOPSUF__TH_AUT_TRANSM", "staging.siops__periodos"],
  kind FULL
);

WITH
    ULTIMAS_TRANSMISSOES AS (
        SELECT
            CO_MUNICIPIO AS IBGE_ENTE,
            NU_ANO,
            NU_PERIODO,
            MAX(NU_SEQ) AS NU_SEQ,
            MAX(DT_HOMOLOGACAO) AS DT_HOMOLOGACAO,
            ST_HOMOLOGADO
        FROM
            RAW.SIOPS__TH_AUT_TRANSM
        GROUP BY ALL
        UNION ALL
        SELECT
            CO_UF AS IBGE_ENTE,
            NU_ANO,
            NU_PERIODO,
            MAX(NU_SEQ) AS NU_SEQ,
            MAX(DT_HOMOLOGACAO) AS DT_HOMOLOGACAO,
            ST_HOMOLOGADO
        FROM
            RAW.SIOPSUF__TH_AUT_TRANSM
        GROUP BY ALL
    ),
    DADOS_HOMOLOGADOS AS (
        SELECT
            *
        FROM
            ULTIMAS_TRANSMISSOES
        WHERE
            ST_HOMOLOGADO = 'S'
    )
SELECT
    p.PERIODO,
    p.ANO,
    p.BIMESTRE,
    h.* exclude (NU_ANO, NU_PERIODO)
FROM
    DADOS_HOMOLOGADOS h
    JOIN staging.siops__periodos p ON h.NU_ANO = p.ANO
    AND h.NU_PERIODO = p.CODIGO_BIMESTRE_SIOPS
ORDER BY
    periodo desc,
    ibge_ente;