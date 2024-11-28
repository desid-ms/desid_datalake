
MODEL (
    name sha.producao_ambulatorial,
    kind FULL
);

SELECT 
    'SIA' as fonte, -- Fonte dos dados: SIA
    left(MAX(a.competencia), 4) as ano_competencia, -- Ano da competencia do registro ambulatorial: YYYY
    CASE 
        WHEN LEFT(a.id_ocupacao_executante_cbo,4) IN 
            ('3522', -- Agentes da saúde e do meio ambiente no br_mte.cbo
            '5151', -- Agentes comunitários de saúde e afins
            ) THEN 'acs' ELSE 'outro'
    END as tipo_profissional, -- Profisional de saúde responsável pelo cuidado é Agente Comunitário de Saúde (ACS) ou outro tipo de profissional
    e.sigla_uf, -- Sigla da UF onde se localiza o estabelecimento provedor do cuidado
    e.id_municipio_ibge, -- Código IBGE de 6 digitos do municipio dp provedor de cuidado 
    e.tipo_provedor, -- Tipo do estabelecimento provedor do cuidado (publico federal, contratado, sem fins lucrativos etc) durante a competencia (mes, ano) do registro ambulatorial
    e.codigo_SHA as HP, -- Código da função HP no SHA do estabelecimento provedor do cuidado
    p.codigo_procedimento as procedimento, -- Código do procedimento realizado no SIGTAP
    p.codigo_sha as HC, -- Código da função HC no SHA do procedimento realizado
    t1.valor as tipo_financiamento, -- Tipo de financiamento da produção: PAB, FAEC, Incentivo MAC, MAC
    p.valor_2022 as valor_unitario, -- Valor unitário em reais nominais do procedimento em 2022 segundo a tabela de procedimentos do SIGTAP
    sum(quantidade_produzida) as n, -- quantidade de procedimentos realizados (produção ambulatorial)
    sum(quantidade_produzida * p.valor_2022) as valor -- valor total em reais nominais da produção ambulatorial
FROM sia.producao a
LEFT JOIN
   sha.provedores e ON CAST(e.id_provedor AS INTEGER) = CAST(a.id_estabelecimento_cnes AS INTEGER) 
   AND CAST(e.competencia AS INTEGER) <= CAST(a.competencia AS INTEGER)
LEFT JOIN
    sha.procedimentos p ON CAST(a.id_procedimento_sigtap AS INTEGER) = CAST(p.codigo_procedimento AS INTEGER)
LEFT JOIN 
   raw.sha__tipos t1 ON CAST(p.codigo_tipo_financiamento AS INTEGER) = CAST(t1.chave AS INTEGER)
   AND t1.id_tabela = 'producao_ambulatorial' 
   AND t1.nome_coluna = 'tipo_financiamento_producao'
WHERE left(a.competencia, 4) = '2022'
GROUP BY ALL
ORDER BY ALL