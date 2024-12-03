-- Produção hospitalar em 2022. Quantidade e valor de internações realizadas em âmbito hospitalar.
MODEL (
    name sha.producao_hospitalar,
    kind FULL
);

SELECT 
    left(last(h.competencia), 4) as ano_competencia, -- Ano de competencia do cuidado realizado: YYYY (Fonte: SIH)
    'SIH ' as fonte, -- Fonte dos dados: SIH (Fonte: NCS/DESID)
    'outro' as tipo_profissional, -- Indicador se a ocupação do profisisonal responsável é agente comunitário. Internações não são realizadas por agentes comunitários (Fonte: NCS/DESID)
    e.sigla_uf, -- Sigla da UF onde se localiza o estabelecimento provedor do cuidado (Fonte: sha.provedores)
    e.id_municipio_ibge, -- Código IBGE de 6 dígitos do município gestor do estabelecimento provedor do cuidado (Fonte: sha.provedores)
    e.tipo_provedor, -- tipo do estabelecimento provedor do cuidado (publico federal, contratado, sem fins lucrativos etc) durante a competencia (mes, ano) do registro ambulatorial (Fonte: sha.provedores)
    e.codigo_SHA as HP, -- Código da função HP no SHA do estabelecimento provedor do cuidado (Fonte: sha.provedores)
    p.codigo_procedimento as procedimento, -- Código no SIGTAP do principal procedimento realizado durante a internação (Fonte: sha.procedimentos) 
    p.codigo_sha as HC, -- Código da função HC no SHA do principal procedimento realizado (Fonte: sha.procedimentos)
    t1.valor as tipo_financiamento,  -- Tipo de financiamento da produção: PAB, FAEC, Incentivo MAC, MAC (Fonte: SIH)
    ROUND(sum(h.valor_internacao)/NULLIF(sum(dias_permanencia),0), 2) as valor_unitario, -- Valor unitário em reais nominais: média do valor de internação (valor internacao/dias de internação) (Fonte: NCS/DESID)
    sum(dias_permanencia)::int as n, -- Quantidade de dias de internação. Inclui produção hospitalar reduzida e rejeitada, pois mesmo que a internação não tenha sido autorizada, o serviço foi prestado e pago pelo SUS (Fonte: SIH)
    sum(h.valor_internacao)::decimal(18,2) as valor -- Valor nominal em reais total da internação segundo o SIH. Inclui valores de diferentes procedimentos além do principal. (Fonte: SIH)
FROM sih.producao h
LEFT JOIN
   sha.provedores e ON CAST(e.id_provedor AS INTEGER) = CAST(h.id_estabelecimento_cnes AS INTEGER) 
   AND CAST(e.competencia AS INTEGER) = CAST(h.competencia AS INTEGER)
LEFT JOIN
    sha.procedimentos p ON CAST(h.procedimento_principal AS INTEGER) = CAST(p.codigo_procedimento AS INTEGER)
LEFT JOIN 
   raw.sha__tipos t1 ON CAST(p.codigo_tipo_financiamento AS INTEGER) = CAST(t1.chave AS INTEGER)
   AND t1.id_tabela = 'producao_ambulatorial' -- tipos de financiamento no SIH são os mesmos do SIA
   AND t1.nome_coluna = 'tipo_financiamento_producao'

GROUP BY ALL
ORDER BY ALL;

 /*  POST-STATEMENT */
@IF(
  @runtime_stage = 'evaluating',
  COPY sha.producao_hospitalar
  TO 'data/outputs/br_ms__sha/producao_hospitalar' WITH (
    FORMAT PARQUET,
    PARTITION_BY (ano_competencia, fonte, sigla_uf),
    OVERWRITE_OR_IGNORE,
    FILENAME_PATTERN 'producao_hospitalar_')
  )