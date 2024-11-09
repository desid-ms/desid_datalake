MODEL (
    name sia.sha,
    kind FULL
);

SELECT 
    'SIA' as fonte,
    left(MAX(a.competencia), 4) as ano_competencia,
    CASE 
        WHEN LEFT(a.cbo_executante,4) IN 
            ('3522', -- Agentes da saúde e do meio ambiente no br_mte.cbo
            '5151', -- Agentes comunitários de saúde e afins
            ) THEN 'acs' ELSE 'outro'
    END as tipo_profissional, -- profisional de saúde responsável pelo cuidado é Agente Comunitário de Saúde (ACS) ou outro tipo de profissional
    a.cnes_estabelecimento as cnes_provedor, -- código do estabelecimento provedor do cuidado no CNES (Cadastro Nacional de Estabelecimentos de Saúde)
    e.tipo_provedor, -- tipo do estabelecimento provedor do cuidado (publico federal, contratado, sem fins lucrativos etc) durante a competencia (mes, ano) do registro ambulatorial
    e.codigo_SHA as HP,
    a.codigo_procedimento,
    p.codigo_sha as HC,
    t1.valor as tipo_financiamento,
    p.valor_2022 as valor_unitario,
    sum(quantidade_produzida) as n,
    sum(quantidade_produzida * p.valor_2022) as valor
FROM sia.producao a
LEFT JOIN
   sha.provedores e ON CAST(e.id_provedor AS INTEGER) = CAST(a.cnes_estabelecimento AS INTEGER) 
   AND CAST(e.competencia AS INTEGER) = CAST(a.competencia AS INTEGER)
LEFT JOIN
    sha.procedimentos p ON CAST(a.codigo_procedimento AS INTEGER) = CAST(p.codigo_procedimento AS INTEGER)
LEFT JOIN 
   raw.sha__tipos t1 ON CAST(p.codigo_tipo_financiamento AS INTEGER) = CAST(t1.chave AS INTEGER)
   AND t1.id_tabela = 'producao_ambulatorial' 
   AND t1.nome_coluna = 'tipo_financiamento_producao'

GROUP BY ALL
ORDER BY ALL

-- PIVOT sha.registros
--       ON tipo_financiamento
--       USING sum(n) as n, sum(valor) as valor
--       GROUP BY fonte, ano_competencia, tipo_profissional, tipo_provedor 
--   ORDER BY ALL