 MODEL (
    name cnes.estabelecimentos,
    kind FULL
);

SELECT
    e.nu_comp AS competencia,
    e.co_cnes AS id_estabelecimento_cnes,
    t1.valor AS tipo_unidade,
    e.co_municipio_gestor as ibge,
    t2.valor AS tipo_gestao,
    t3.valor AS tipo_pessoa,
    t4.valor AS tipo_natureza_administrativa,
    e.co_natureza_jur AS codigo_natureza_juridica,
    -- sha.provedor já tem
    -- COALESCE(
    --     CASE -- procurar fonte, codigos vieram do script Raulino
    --         WHEN e.co_natureza_jur IN ('1015', '1040', '1074', '1104', '1139', '1163', '1252') THEN 'publico federal'
    --         WHEN e.co_natureza_jur IN ('1023', '1058', '1082', '1112', '1147', '1171', '1236', '1260') THEN 'publico estadual'
    --         WHEN e.co_natureza_jur IN ('1031', '1066', '1120', '1155', '1180', '1244', '1279') THEN 'publico municipal'
    --         WHEN e.co_natureza_jur = '3131' THEN 'sindicato'
    --         WHEN e.co_natureza_jur BETWEEN '1000' AND '1999' THEN 'publico'
    --         WHEN e.co_natureza_jur BETWEEN '2000' AND '2999' THEN 'privado'
    --         WHEN e.co_natureza_jur BETWEEN '3000' AND '3999' THEN 'entidade beneficente sem fins lucrativos'
    --         WHEN e.co_natureza_jur IN ('4000', '4014') THEN 'privado'
    --         ELSE t4.valor
    --     END,
    --     'desconhecido'
    -- ) AS tipo_prestador,
    e.NU_CNPJ_MANTENEDORA AS cnpj_mantenedora,
    e.NO_FANTASIA AS nome_fantasia_estabecimento,
    e.NU_LATITUDE AS latitude_estabelecimento,
    e.NU_LONGITUDE AS longitude_estabelecimento
FROM
    raw.cnes__tb_comp_estabelecimento e
    LEFT JOIN raw.cnes__tipos t1 ON e.tp_unidade::int = t1.chave::int
        AND t1.id_tabela = 'estabelecimento' AND t1.nome_coluna = 'tipo_unidade'
    LEFT JOIN raw.cnes__tipos t2 ON e.tp_gestao = t2.chave
        AND t2.id_tabela = 'estabelecimento' AND t2.nome_coluna = 'tipo_gestao'
    LEFT JOIN raw.cnes__tipos t3 ON e.tp_pfpj = t3.chave
        AND t3.id_tabela = 'estabelecimento' AND t3.nome_coluna = 'tipo_pessoa'
    LEFT JOIN raw.cnes__tipos t4 ON e.co_natureza_organizacao::int = t4.chave::int
        AND t4.id_tabela = 'estabelecimento' AND t4.nome_coluna = 'tipo_natureza_administrativa';
