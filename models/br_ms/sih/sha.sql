MODEL (
    name sih.sha,
    kind FULL
);

SELECT 
    'SIH' as fonte,
    left(last(p.competencia_internacao), 4) as competencia,
    p.cnes_provedor,
    'outro' as cbo,
    1 as saude,
    p.tipo_provedor_competencia as tipo_provedor, -- tipo do provedor (publico federal, contratado, sem fins lucrativos etc)
    p.tipo_financiamento,
    p.codigo_sha_provedor_competencia as HP,
    null as HF,
    p.HC,
    sum(quantidade_dias_permanencia)::int as quantidade,
    sum(valor_total)::decimal(38,2) as valor
FROM sih.producao_hospitalar p
GROUP BY p.cnes_provedor, cbo, saude, p.tipo_provedor_competencia, p.tipo_financiamento, HP, HF, HC


