MODEL (
    name sia.sha,
    kind FULL
);

SELECT 
    'SIA' as fonte,
    left(last(p.competencia), 4) as competencia,
    p.cnes_estabelecimento as cnes_provedor,
    CASE 
        WHEN p.cbo_executante IN ('352210','515105','515120','515125','515130') THEN 'acs' 
        ELSE 'outro'
    END as cbo,
    CASE 
        WHEN p.cbo_executante IN ('322415','351605','352210','515105','515120','515125','515130') THEN 0 
        ELSE 1
    END as saude,
    p.tipo_provedor_competencia as tipo_provedor,
    p.tipo_financiamento_producao as tipo_financiamento,
    p.codigo_sha_estabelecimento_competencia as HP,
    null as HF,
    p.codigo_sha_procedimento as HC,
    sum(quantidade_produzida)::int as quantidade,
    sum(valor_produzido)::decimal(38,2) as valor
FROM sia.producao_ambulatorial p
GROUP BY cnes_provedor, cbo, saude, tipo_provedor_competencia, tipo_financiamento_producao, HP, HF, HC

