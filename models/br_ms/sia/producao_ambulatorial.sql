-- Produção ambulatorial do SIA com dados extraídos da Base dos Dados/TABNET
MODEL (
    name sia.producao,
    kind FULL
);


SELECT
   pa.pa_cmp as competencia, -- ano e mês da producao ambulatorial
   pa.pa_gestao as id_municipio_ibge, -- código IBGE do município gestor
   pa.pa_coduni as id_estabelecimento_cnes, -- id do estabelecimento no CNES
   pa.pa_cbocod as id_ocupacao_executante_cbo, -- código brasileiro de ocupações (CBO) do profissional de saúde executante
   pa.pa_proc_id AS id_procedimento_sigtap, -- código do procedimento realizado no SIGTAP
   pa.pa_tpfin  as codigo_tipo_financiamento, 
   CASE pa.pa_tpfin 
      WHEN '01' THEN 'PAB'
      WHEN '04' THEN 'FAEC'
      WHEN '05' THEN 'Incentivo MAC'
      WHEN '06' THEN 'MAC'
      ELSE 'INVÁLIDO'
   END as tipo_financiamento, -- código do tipo de financimento da produção 
   pa.PA_QTDPRO::int as quantidade_produzida, -- quantidade de procedimentos realizados
   pa.PA_QTDAPR::int as quantidade_aprovada, -- quantidade de procedimentos realizados
FROM
   raw.sia__producao_bd pa

 WHERE 
   pa.pa_tpfin not in 
      (
         '02', -- assitencia farmaceutica 
         '07'  -- vigilancia em saude
      )
   and (PA_QTDPRO::int > 0 or PA_QTDAPR::int > 0)
 