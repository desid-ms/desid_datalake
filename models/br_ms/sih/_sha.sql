MODEL (
    name sih.sha,
    kind FULL
);

SELECT 
    'SIH' as fonte,
    left(last(h.competencia), 4) as ano_competencia,
    'outro' as tipo_profissional, -- internações não são realizadas por agentes comunitários
    cnes_provedor, -- código do estabelecimento provedor do cuidado no CNES (Cadastro Nacional de Estabelecimentos de Saúde)
    e.tipo_provedor, -- tipo do estabelecimento provedor do cuidado (publico federal, contratado, sem fins lucrativos etc) durante a competencia (mes, ano) do registro ambulatorial
    -- e.tipo_gestao,
    e.codigo_SHA as HP,
    p.codigo_procedimento,
    p.codigo_sha as HC,
    t1.valor as tipo_financiamento,
    sum(dias_permanencia)::int as n,
    sum(h.valor_internacao) as valor
FROM sih.producao h
LEFT JOIN
   sha.provedores e ON CAST(e.id_provedor AS INTEGER) = CAST(h.cnes_provedor AS INTEGER) 
   AND CAST(e.competencia AS INTEGER) = CAST(h.competencia AS INTEGER)
LEFT JOIN
    sha.procedimentos p ON CAST(h.codigo_procedimento_principal AS INTEGER) = CAST(p.codigo_procedimento AS INTEGER)
LEFT JOIN 
   raw.sha__tipos t1 ON CAST(p.codigo_tipo_financiamento AS INTEGER) = CAST(t1.chave AS INTEGER)
   AND t1.id_tabela = 'producao_ambulatorial' -- tipos de financiamento no SIH são os mesmos do SIA
   AND t1.nome_coluna = 'tipo_financiamento_producao'

GROUP BY ALL
ORDER BY ALL
-- SELECT 
--     'SIH' as fonte,
--     left(last(p.competencia_internacao), 4) as competencia,
--     p.cnes_provedor,
--     'outro' as cbo,
--     1 as saude,
--     p.tipo_provedor_competencia as tipo_provedor, -- tipo do provedor (publico federal, contratado, sem fins lucrativos etc)
--     p.tipo_financiamento,
--     p.codigo_sha_provedor_competencia as HP,
--     null as HF,
--     p.HC,
--     sum(quantidade_dias_permanencia)::int as quantidade,
--     sum(valor_total)::decimal(38,2) as valor
-- FROM sih.producao_hospitalar p
-- GROUP BY p.cnes_provedor, cbo, saude, p.tipo_provedor_competencia, p.tipo_financiamento, HP, HF, HC


