 MODEL (
    name sia.producao_ambulatorial,
    kind FULL
);

SELECT
   pa.co_pa_cmp as competencia,
   pa.co_pa_coduni as cnes_estabelecimento, -- id do estabelecimento no CNES
   pa.co_pa_cbocod as cbo_executante, -- código brasileiro de ocupações (CBO) do profissional de saúde executante
   o.descricao AS profissional_saude_executante, -- descrição da ocupação do profissional de saúde executante no CBO
   pa.co_pa_proc_id AS id_procedimento, -- código do procedimento realizado no SIGTAP
   proc.procedimento_sus as descricao_procedimento, -- descrição do procedimento correspondente no SIGTAP
   pa.co_pa_tpfin as codigo_tipo_financiamento, -- código do tipo de financimento da produção
   t1.valor as tipo_financiamento_producao, -- descrição do tipo de financiamento da produção
   f.ValorPAB2022::decimal(18,2) as valor_pab_procedimento, -- valor procedimento de atenção básica em 2022
   proc.codigo_sha as codigo_sha_procedimento, -- código sha HC correspondente ao cuidado do procedimento
   pa.SUM_NU_PA_QTDPRO as quantidade_produzida,
   pa.SUM_NU_PA_QTDAPR as quantidade_aprovada,
   pa.SUM_NU_PA_VALPRO::decimal(18,2) as valor_produzido,
   pa.SUM_NU_PA_VALAPR::decimal(18,2) as valor_aprovado,
   (pa.SUM_NU_PA_TOT/pa.LINHAS)::decimal(18,2) as valor_unitario_tabela_sigtap
 FROM
   raw.sia__tb_pa pa
 LEFT JOIN
   br_mte.cbo o ON pa.co_pa_cbocod = o.cbo_2002
 LEFT JOIN
    raw.sia__valor_pab f on f.proc_rea = pa.co_pa_proc_id
 LEFT JOIN
   sha.procedimentos proc ON pa.co_pa_proc_id = proc.id_procedimento_sigtap::text
 LEFT JOIN raw.sia__tipos t1 ON pa.co_pa_tpfin::int = t1.chave::int
        AND t1.id_tabela = 'producao_ambulatorial' AND t1.nome_coluna = 'tipo_financiamento_producao'