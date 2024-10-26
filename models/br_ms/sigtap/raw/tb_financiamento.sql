MODEL (
    name raw.sigtap__tb_financiamento,
    kind FULL
);

SELECT
    SUBSTRING(COL1, 1, 2)::int as CO_FINANCIAMENTO,
    TRIM(SUBSTRING(COL1, 3, 90)) as NO_FINANCIAMENTO,
    SUBSTRING(COL1, 93, 6) as DT_COMPETENCIA
FROM read_csv(
    'data/inputs/sha/SIGTAP/tb_financiamento.txt',
    header = false,
    columns = {'COL1':'TEXT'}
);