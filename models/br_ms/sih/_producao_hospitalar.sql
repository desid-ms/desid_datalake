MODEL (
    name sih.producao_hospitalar,
    kind FULL
);

 WITH DEDUPLICATED AS (
 SELECT
    last (dt_cmpt) as competencia,
    nu_aih as id_aih,
    last (co_ident) as co_ident, -- ?
    last (co_cnes) as id_estabelecimento, -- código cnes
    last (NU_ESPECIALIDADE) as especialidade,
    last (CO_PROC_REALIZADO) as id_procedimento,
    last (nu_natjur) as codigo_natjur,
    last (CO_FINANCIAMENTO) as codigo_tipo_financiamento,
    sum (nu_val_tot) as valor_total,
    last (dt_internacao) as data_internacao,
    last (dt_saida) as data_saida 
FROM
    raw.sih__tb_reduz_rejeit
GROUP BY nu_aih)
    SELECT 
        competencia,
        id_aih,
        co_ident,
        id_estabelecimento,
        especialidade,
        id_procedimento,
        proc.codigo_sha as codigo_sha_procedimento,
        codigo_natjur,
        n.descricao AS natureza_juridica,
        codigo_tipo_financiamento,
        t1.valor as tipo_financiamento_producao,
        valor_total,
        data_internacao,
        data_saida

    FROM
    DEDUPLICATED ph
    LEFT JOIN raw.sia__tipos t1 ON ph.codigo_tipo_financiamento::int = t1.chave::int
        AND t1.id_tabela = 'producao_ambulatorial' AND t1.nome_coluna = 'tipo_financiamento_producao'
    LEFT JOIN
        sha.procedimentos proc ON ph.id_procedimento = proc.id_procedimento_sigtap
     LEFT JOIN raw.cnes__naturezas_juridicas n ON ph.codigo_natjur = n.id_natureza_juridica    