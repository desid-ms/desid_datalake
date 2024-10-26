MODEL (
    name sha.procedimentos
);

SELECT 
    s.dt_competencia as competencia,
    s.co_procedimento::int as codigo_procedimento,
    TRIM(s.no_procedimento) as procedimento,
    hc.codigo_sha,
    s.co_financiamento as codigo_tipo_financiamento,
    f.no_financiamento as tipo_financiamento 
FROM raw.sigtap__tb_procedimentos s 
LEFT JOIN raw.sha__hc_sigtap hc 
    ON hc.codigo_procedimento_sigtap::int = s.co_procedimento::int  
LEFT JOIN raw.sigtap__tb_financiamento f
    ON f.co_financiamento::int = s.co_financiamento::int
