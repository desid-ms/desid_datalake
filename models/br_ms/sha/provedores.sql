 MODEL (
    name sha.provedores,
    kind FULL
);

SELECT
    e.nu_comp AS competencia,
    e.co_cnes AS id_provedor, -- código cnes do estabelevimento provedor de cuidados
    h.hp_code as codigo_sha,
    e.tp_unidade AS codigo_tpups, -- código cnes do tipo da unidade (estabelecimento de saúde)
    t1.valor AS tipo_unidade,
    n.id_natureza_juridica AS codigo_natjur, --código cnes da natureza jurídica 
    n.descricao AS natureza_juridica,
    e.tp_gestao AS codigo_tpgestao, --código cnes do tipo de gestão
    t2.valor AS tipo_gestao
FROM
    raw.cnes__tb_comp_estabelecimento e
    LEFT JOIN raw.cnes__tipos t1 ON e.tp_unidade = t1.chave
        AND t1.id_tabela = 'estabelecimento' AND t1.nome_coluna = 'tipo_unidade'
    LEFT JOIN raw.cnes__tipos t2 ON e.tp_gestao = t2.chave
        AND t2.id_tabela = 'estabelecimento' AND t2.nome_coluna = 'tipo_gestao'
    LEFT JOIN raw.cnes__tipos t3 ON e.tp_pfpj = t3.chave
        AND t3.id_tabela = 'estabelecimento' AND t3.nome_coluna = 'tipo_pessoa'
    LEFT JOIN raw.cnes__naturezas_juridicas n ON e.co_natureza_jur = n.id_natureza_juridica
    LEFT JOIN raw.sha__tpups_hps h ON h.TPUPS = e.tp_unidade
