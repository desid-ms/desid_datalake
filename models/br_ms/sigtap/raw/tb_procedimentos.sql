
MODEL (
    name raw.sigtap__tb_procedimentos,
    kind FULL
);
SELECT
    SUBSTRING(COL1, 1, 10) as CO_PROCEDIMENTO,
    SUBSTRING(COL1, 11, 250) as NO_PROCEDIMENTO,
    SUBSTRING(COL1, 261, 1)::int as TP_COMPLEXIDADE,
    SUBSTRING(COL1, 262, 1) as TP_SEXO,
    CAST(SUBSTRING(COL1, 263, 4) AS INTEGER) as QT_MAXIMA_EXECUCAO,
    CAST(SUBSTRING(COL1, 267, 4) AS INTEGER) as QT_DIAS_PERMANENCIA,
    CAST(SUBSTRING(COL1, 271, 4) AS INTEGER) as QT_PONTOS,
    CAST(SUBSTRING(COL1, 275, 4) AS INTEGER) as VL_IDADE_MINIMA,
    CAST(SUBSTRING(COL1, 279, 4) AS INTEGER) as VL_IDADE_MAXIMA,
    CAST(SUBSTRING(COL1, 283, 10) AS DECIMAL) as VL_SH,
    CAST(SUBSTRING(COL1, 293, 10) AS DECIMAL) as VL_SA,
    CAST(SUBSTRING(COL1, 303, 10) AS DECIMAL) as VL_SP,
    SUBSTRING(COL1, 313, 2)::int as CO_FINANCIAMENTO,
    SUBSTRING(COL1, 315, 6) as CO_RUBRICA,
    CAST(SUBSTRING(COL1, 321, 4) AS INTEGER) as QT_TEMPO_PERMANENCIA,
    SUBSTRING(COL1, 325, 6) as DT_COMPETENCIA
FROM read_csv('data/inputs/sha/SIGTAP/tb_procedimento.txt', header = false, columns = {'COL1':'TEXT'});