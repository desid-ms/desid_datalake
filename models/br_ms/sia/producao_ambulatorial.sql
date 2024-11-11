MODEL (
    name sia.producao,
    kind FULL
);

SELECT
   pa.co_pa_cmp as competencia, -- ano e mês da producao ambulatorial
   pa.co_pa_coduni as cnes_estabelecimento, -- id do estabelecimento no CNES
   t.nome_uf as nome_uf_estabelecimento, -- nome da UF do estabelecimento
   t.sigla_uf as uf_estabelecimento, -- sigla da UF do estabelecimento
   pa.co_pa_cbocod as cbo_executante, -- código brasileiro de ocupações (CBO) do profissional de saúde executante
   pa.co_pa_proc_id AS codigo_procedimento, -- código do procedimento realizado no SIGTAP
   pa.co_pa_tpfin  as codigo_tipo_financiamento, 
   CASE pa.co_pa_tpfin 
      WHEN '01' THEN 'PAB'
      WHEN '04' THEN 'FAEC'
      WHEN '05' THEN 'Incentivo MAC'
      WHEN '06' THEN 'MAC'
      ELSE 'INVÁLIDO'
   END as tipo_financiamento, -- código do tipo de financimento da produção 
   pa.SUM_NU_PA_QTDPRO as quantidade_produzida, -- quantidade de procedimentos realizados
   pa.SUM_NU_PA_QTDAPR as quantidade_aprovada, -- quantidade de procedimentos realizados
FROM
   raw.sia__tb_pa pa
   LEFT JOIN br_ms.territorio t on t.codigo_municipio = pa.co_pa_gestao
 WHERE 
   pa.co_pa_tpfin not in 
      (
         '02', -- assitencia farmaceutica 
         '07'  -- vigilancia em saude
      )
   and (pa.SUM_NU_PA_QTDPRO > 0 or pa.SUM_NU_PA_QTDAPR > 0)
 