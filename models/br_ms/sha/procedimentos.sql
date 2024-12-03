-- Dados sobre os procedimentos realizados no SUS, cuidados, de acordo com a metodologia do SHA (HC, Health Care)
MODEL (
    name sha.procedimentos
);
select 
    '2022' as ano, -- Ano de competencia do valor do procedimento (Fonte: SIGTAP, NCS/DESID)
    coalesce(p.proc_rea, LPAD(c.codigo_sigtap::VARCHAR, 10, '0')) as codigo_procedimento, -- Cödigo do procedimento realizado (Fonte: SIGTAP, NCS/DESID)
    coalesce(p.nome_proc, trim(c.nome_procedimento_2022)) as procedimento,  -- Nome do procedimento realizado (Fonte: SIGTAP, NCS/DESID)
    coalesce(p.fonte, 'NCS') as fonte, -- Indica se a fonte da informação é NCS ou SIGTAP (nulo) (Fonte: NCS/DESID)
    c.codigo_sha, -- Código SHA do cuidado (HC) relativo ao procedimento (Fonte: NCS/DESID)
    p.co_financiamento as codigo_tipo_financiamento, -- Código do tipo de financiamento do procedimento (Fonte: SIGTAP, NCS/DESID)
    try_cast(p.valor_2022 as decimal(18,2)) as valor -- Valor do procedimento em reais nominais  (Fonte: SIGTAP, NCS/DESID)
from 
    read_csv_auto('data/inputs/sha/valor_sigtap_ncs.csv') p 
full outer join 
    raw.sha__HC_CAES c on LPAD(c.codigo_sigtap::VARCHAR, 10, '0') = p.proc_rea;


 /*  POST-STATEMENT */
@IF(
  @runtime_stage = 'evaluating',
  COPY sha.procedimentos
  TO 'data/outputs/br_ms__sha/procedimentos' WITH (
    FORMAT PARQUET,
    PARTITION_BY (ano),
    OVERWRITE_OR_IGNORE,
    FILENAME_PATTERN 'procedimentos_')
  );

