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
   e.co_natureza_jur AS codigo_natureza_juridica, --código cnes da natureza jurídica 
   n.descricao AS natureza_juridica,
   COALESCE(
       CASE 
           WHEN e.co_natureza_jur IN ('1015', '1040', '1074', '1104', '1139', '1163', '1252') THEN 'federal'
           WHEN e.co_natureza_jur IN ('1023', '1058', '1082', '1112', '1147', '1171', '1236', '1260') THEN 'estadual'
           WHEN e.co_natureza_jur IN ('1031', '1066', '1120', '1155', '1180', '1244', '1279') THEN 'municipal'
           WHEN e.co_natureza_jur IN ('1198', '1201', '1210', '1228') AND e.tp_gestao = 'M' THEN 'municipal'
           WHEN e.co_natureza_jur IN ('1198', '1201', '1210', '1228') AND e.tp_gestao = 'E' THEN 'estadual'
           WHEN e.co_natureza_jur IN ('1198', '1201', '1210', '1228') THEN 'outra entidade pública'
           WHEN e.co_natureza_jur BETWEEN '2000' and '2999' THEN 'contratado'
           WHEN e.co_natureza_jur BETWEEN '3000' and '2999' THEN 'sem fins lucrativos'
       END,
    'ignorado'
   ) AS tipo_gasto,
   e.tp_gestao AS codigo_tipo_gestao, --código cnes do tipo de gestão 
   t2.valor AS tipo_gestao
FROM
   raw.cnes__tb_comp_estabelecimento e
   LEFT JOIN raw.cnes__tipos t1 ON e.tp_unidade::int = t1.chave::int
       AND t1.id_tabela = 'estabelecimento' AND t1.nome_coluna = 'tipo_unidade'
   LEFT JOIN raw.cnes__tipos t2 ON e.tp_gestao = t2.chave
       AND t2.id_tabela = 'estabelecimento' AND t2.nome_coluna = 'tipo_gestao'
   LEFT JOIN raw.cnes__tipos t3 ON e.tp_pfpj::int = t3.chave::int
       AND t3.id_tabela = 'estabelecimento' AND t3.nome_coluna = 'tipo_pessoa'
   LEFT JOIN raw.cnes__naturezas_juridicas n ON e.co_natureza_jur::int = n.id_natureza_juridica::int
   LEFT JOIN raw.sha__tpups_hps h ON h.TPUPS::int = e.tp_unidade::int
