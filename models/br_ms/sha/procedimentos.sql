MODEL (
    name sha.procedimentos
);

SELECT 
    c.codigo_sigtap::int as codigo_procedimento,
    -- TRIM(c.nome_procedimento_2022) as procedimento,
    TRIM(p.no_procedimento) as procedimento,
    p.co_financiamento as codigo_tipo_financiamento,
    c.codigo_sha 
FROM 
    raw.sha__HC_CAES c 
LEFT JOIN 
    raw.sigtap__tb_procedimentos p 
    ON c.codigo_sigtap = CAST(p.co_procedimento AS INTEGER)