MODEL (
    name sia.sha,
    kind FULL
);

SELECT 
    'SIA' as fonte,
    left(last(p.competencia), 4) as competencia,
    p.cnes_estabelecimento as cnes_provedor,
    CASE 
        WHEN p.cbo_executante IN ('352210','515105','515120','515125','515130') THEN 'acs' 
        ELSE 'outro'
    END as cbo,
    CASE 
        WHEN p.cbo_executante IN ('322415','351605','352210','515105','515120','515125','515130') THEN 0 
        ELSE 1
    END as saude,
    p.tipo_provedor_competencia as tipo_provedor,
    p.tipo_financiamento_producao as tipo_financiamento,
    p.codigo_sha_estabelecimento_competencia as HP,
    null as HF,
    p.codigo_sha_procedimento as HC,
    sum(quantidade_produzida) as quantidade,
    sum(valor_produzido) as valor
FROM sia.producao_ambulatorial p
GROUP BY cnes_provedor, cbo, saude, tipo_provedor_competencia, tipo_financiamento_producao, HP, HF, HC

-- SELECT
--    pa.co_pa_cmp as competencia, -- ano e mês da producao ambulatorial
--    pa.co_pa_coduni as cnes_estabelecimento, -- id do estabelecimento no CNES
--    e.codigo_sha as codigo_sha_estabelecimento_competencia, -- codigo sha do estabelecimento no momento da competencia
--    pa.co_pa_cbocod as cbo_executante, -- código brasileiro de ocupações (CBO) do profissional de saúde executante
--    o.descricao AS profissional_saude_executante, -- descrição da ocupação do profissional de saúde executante no CBO
--    pa.co_pa_proc_id AS codigo_procedimento, -- código do procedimento realizado no SIGTAP
--    proc.procedimento, -- descrição do procedimento correspondente no SIGTAP
--    pa.co_pa_tpfin as codigo_tipo_financiamento, -- código do tipo de financimento da produção
--    t1.valor as tipo_financiamento_producao, -- descrição do tipo de financiamento da produção
--    f.ValorPAB2022::decimal(18,2) as valor_pab_procedimento, -- valor procedimento de atenção básica em 2022
--    proc.codigo_sha as codigo_sha_procedimento, -- código sha HC correspondente ao cuidado do procedimento
--    pa.SUM_NU_PA_QTDPRO as quantidade_produzida,
--    pa.SUM_NU_PA_QTDAPR as quantidade_aprovada,
--    pa.SUM_NU_PA_VALPRO::decimal(18,2) as valor_produzido,
--    pa.SUM_NU_PA_VALAPR::decimal(18,2) as valor_aprovado,
--    (valor_produzido/quantidade_produzida) as valor_unitario, -- valor_produzido/quantidade_produzida
--    (pa.SUM_NU_PA_TOT/pa.LINHAS)::decimal(18,2) as valor_unitario_tabela_sigtap 
--  FROM
--    raw.sia__tb_pa pa
--  LEFT JOIN
--    br_mte.cbo o ON LPAD(pa.co_pa_cbocod, 6, '0') = LPAD(o.cbo_2002, 6, '0')  -- Left Padding para lidar com os zeros
--  LEFT JOIN
--     raw.sia__valor_pab f on f.proc_rea = pa.co_pa_proc_id
--  LEFT JOIN raw.sha__tipos t1 ON pa.co_pa_tpfin::int = t1.chave::int
--         AND t1.id_tabela = 'producao_ambulatorial' AND t1.nome_coluna = 'tipo_financiamento_producao'
--  LEFT JOIN
--    sha.procedimentos proc ON pa.co_pa_proc_id::int = proc.codigo_procedimento::int
--  LEFT JOIN
--    sha.provedores e ON e.id_provedor::int = pa.co_pa_coduni::int AND e.competencia::int = pa.co_pa_cmp::int
 