MODEL (
    name sia.producao,
    kind FULL
);

SELECT
   pa.co_pa_cmp as competencia, -- ano e mês da producao ambulatorial
   pa.co_pa_coduni as cnes_estabelecimento, -- id do estabelecimento no CNES
   pa.co_pa_cbocod as cbo_executante, -- código brasileiro de ocupações (CBO) do profissional de saúde executante
   pa.co_pa_proc_id AS codigo_procedimento, -- código do procedimento realizado no SIGTAP
   pa.SUM_NU_PA_QTDPRO as quantidade_produzida, -- quantidade de procedimentos realizados
FROM
   raw.sia__tb_pa pa
WHERE 
   pa.co_pa_tpfin not in ('02', '07')
   AND pa.SUM_NU_PA_QTDPRO > 0
 

-- SELECT
--    pa.co_pa_cmp as competencia, -- ano e mês da producao ambulatorial
--    pa.co_pa_coduni as cnes_estabelecimento, -- id do estabelecimento no CNES
--    -- e.codigo_sha as codigo_sha_estabelecimento_competencia, -- codigo sha do estabelecimento no momento da competencia
--    -- e.tipo_provedor as tipo_provedor_competencia, -- tipo do provedor (publico federal, contratado, sem fins lucrativos etc)
--    pa.co_pa_cbocod as cbo_executante, -- código brasileiro de ocupações (CBO) do profissional de saúde executante
--    o.descricao AS profissional_saude_executante, -- descrição da ocupação do profissional de saúde executante no CBO
--    pa.co_pa_proc_id AS codigo_procedimento, -- código do procedimento realizado no SIGTAP
--    -- proc.procedimento, -- descrição do procedimento correspondente no SIGTAP
--    -- pa.co_pa_tpfin as codigo_tipo_financiamento, -- código do tipo de financimento da produção
--    -- t1.valor as tipo_financiamento_producao, -- descrição do tipo de financiamento da produção
--    -- f.ValorPAB2022::decimal(18,2) as valor_pab_procedimento, -- valor procedimento de atenção básica em 2022
--    -- proc.codigo_sha as codigo_sha_procedimento, -- código sha HC correspondente ao cuidado do procedimento
--    pa.SUM_NU_PA_QTDPRO as quantidade_produzida,
--    -- pa.SUM_NU_PA_QTDAPR as quantidade_aprovada,
--    -- pa.SUM_NU_PA_VALPRO::decimal(18,2) as valor_produzido,
--    -- pa.SUM_NU_PA_VALAPR::decimal(18,2) as valor_aprovado,
--   --  (valor_produzido/quantidade_produzida) as valor_unitario, -- valor_produzido/quantidade_produzida
--   --  (pa.SUM_NU_PA_TOT/pa.LINHAS)::decimal(18,2) as valor_unitario_tabela_sigtap 
-- FROM
--    raw.sia__tb_pa pa
-- -- LEFT JOIN
-- --    br_mte.cbo o ON LPAD(pa.co_pa_cbocod, 6, '0') = LPAD(o.cbo_2002, 6, '0')
-- -- LEFT JOIN
-- --    raw.sia__valor_pab f on f.proc_rea = pa.co_pa_proc_id
-- -- LEFT JOIN 
-- --    raw.sha__tipos t1 ON CAST(pa.co_pa_tpfin AS INTEGER) = CAST(t1.chave AS INTEGER)
-- --    AND t1.id_tabela = 'producao_ambulatorial' 
-- --    AND t1.nome_coluna = 'tipo_financiamento_producao'
-- -- LEFT JOIN
-- --   sha.procedimentos proc ON CAST(pa.co_pa_proc_id AS INTEGER) = CAST(proc.codigo_procedimento AS INTEGER)
-- -- LEFT JOIN
-- --    sha.provedores e ON CAST(e.id_provedor AS INTEGER) = CAST(pa.co_pa_coduni AS INTEGER) 
-- --    AND CAST(e.competencia AS INTEGER) = CAST(pa.co_pa_cmp AS INTEGER)
-- WHERE 
--    pa.co_pa_tpfin not in ('02', '07')
--    AND pa.SUM_NU_PA_QTDPRO > 0
 