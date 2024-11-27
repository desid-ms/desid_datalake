MODEL (
    name sha.registros
);

SELECT * exclude(valor_unitario) from sia.sha
UNION
SELECT * from sih.sha



-- copy (select * exclude(valor), valor::double as valor from sha.registros) to 'data/outputs/sha/sha_outputs.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');

-- with sha_pivot as (PIVOT sha.registros
--         ON tipo_financiamento
--         USING sum(n) as n, sum(valor) as valor
--         GROUP BY fonte, ano_competencia, tipo_profissional, tipo_provedor 
--   ORDER BY ALL) 

-- copy (
--   SELECT 
--     fonte,
--     ano_competencia,
--     tipo_profissional,
--     tipo_provedor,
--     FAEC_n::bigint as FAEC_n,
--     FAEC_valor::DOUBLE AS FAEC_valor,
--     "Incentivo MAC_n"::bigint as "Incentivo MAC_n",
--     "Incentivo MAC_valor"::DOUBLE AS "Incentivo MAC_valor",
--     MAC_n::bigint as MAC_n,
--     MAC_valor::DOUBLE AS MAC_valor,
--     PAB_n::bigint as PAB_n,
--     PAB_valor::DOUBLE AS PAB_valor
-- FROM (
--     PIVOT sha.registros
--     ON tipo_financiamento
--     USING sum(n) as n, sum(valor) as valor
--     GROUP BY fonte, ano_competencia, tipo_profissional, tipo_provedor
--     ORDER BY ALL
-- )) to 'data/outputs/sha/sha_consolidado.xlsx' WITH (FORMAT GDAL, DRIVER 'xlsx');