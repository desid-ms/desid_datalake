MODEL (
  name raw.sha__procedimentos,
  kind FULL
);

select 
    coalesce(p.proc_rea, LPAD(c.codigo_sigtap::VARCHAR, 10, '0')) as codigo_procedimento,
    coalesce(p.nome_proc, trim(c.nome_procedimento_2022)) as procedimento,  
    coalesce(p.fonte, 'CAES') as fonte,
    c.codigo_sha,
    p.co_financiamento as codigo_tipo_financiamento,
    try_cast(p.valor_2022 as decimal(18,2)) as valor_2022
from 
    read_csv_auto('data/inputs/sha/valor_sigtap_raulino.csv') p 
full outer join 
    raw.sha__HC_CAES c on LPAD(c.codigo_sigtap::VARCHAR, 10, '0') = p.proc_rea

