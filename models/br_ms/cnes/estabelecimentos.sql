 MODEL (
    name cnes.estabelecimentos,
    kind FULL
);

WITH UFs as (
   select distinct nome_regiao, sigla_uf, nome_uf, left(codigo_municipio::text, 2) as codigo_uf from br_ms.territorio order by sigla_uf
)
SELECT
    e.nu_comp AS competencia, -- Ano e mês da competência do registro do estabelecimento: YYYYMM
    e.co_cnes AS id_estabelecimento_cnes, -- Código do estabelecimento no CNES (de 01 a 85)
    e.tp_unidade as codigo_tipo_estabelecimento, -- Cödigo CNES do tipo de estabelecimento
    t1.valor AS tipo_estabelecimento, -- Tipo de estabelecimento (ex. pronto socorro geral, hospital geral, centro de imunizacao etc)
    t2.valor AS tipo_gestao, -- Tipo de gestão: estadual, municipal, dupla, sem gestao, nao informado
    t3.valor AS tipo_pessoa, -- Indicador se o estabelecimento é pessoa física ou jurídica
    e.co_natureza_jur AS codigo_natureza_juridica, -- Código da natureza jurídica do estabelecimento
    COALESCE(lower(n.descricao), 
        CASE
            WHEN e.co_natureza_jur between '1000' and '1999' THEN 'administracao publica'
            WHEN e.co_natureza_jur between '2000' and '2999' THEN 'entidade empresarial'
            WHEN e.co_natureza_jur between '3000' and '3999' THEN 'entidade sem fins lucrativos'
            WHEN e.co_natureza_jur between '4000' and '4999' THEN 'pessoa fisica'
            WHEN e.co_natureza_jur between '5000' and '5999' THEN 'instituicao extraterritorial'
            ELSE 'invalida' -- nao conformidade com a tabela de natureza juridica do CONCLA (Conselho Nacional de Classificação)
        END
     ) AS natureza_juridica, -- Natureza jurídica do estabelecimento 
    e.NU_CNPJ_MANTENEDORA AS cnpj_mantenedora, -- CNPJ da mantenedora do estabelecimento
    e.NO_FANTASIA AS nome_fantasia_estabelecimento, -- Nome fantasia do estabelecimento
    e.co_municipio_gestor as  id_municipio_ibge, -- Código IBGE de 6 digitos do município gestor do estabelecimento
    UFs.sigla_uf as sigla_uf, -- Sigla da UF onde se localiza o estabelecimento
    e.NU_LATITUDE AS latitude_estabelecimento, -- Latitude do estabelecimento em graus
    e.NU_LONGITUDE AS longitude_estabelecimento -- Longitude do estabelecimento em graus
FROM
    raw.cnes__tb_comp_estabelecimento e
    LEFT JOIN UFs ON left(e.co_municipio_gestor::text, 2) = UFs.codigo_uf
    LEFT JOIN raw.cnes__tipos t1 ON e.tp_unidade::int = t1.chave::int
        AND t1.id_tabela = 'estabelecimento' AND t1.nome_coluna = 'tipo_unidade'
    LEFT JOIN raw.cnes__tipos t2 ON e.tp_gestao = t2.chave
        AND t2.id_tabela = 'estabelecimento' AND t2.nome_coluna = 'tipo_gestao'
    LEFT JOIN raw.cnes__tipos t3 ON e.tp_pfpj = t3.chave
        AND t3.id_tabela = 'estabelecimento' AND t3.nome_coluna = 'tipo_pessoa'
    LEFT JOIN raw.cnes__tipos t4 ON e.co_natureza_organizacao::int = t4.chave::int
        AND t4.id_tabela = 'estabelecimento' AND t4.nome_coluna = 'tipo_natureza_administrativa'
    LEFT JOIN raw.cnes__tipos t5 ON e.tp_prestador::int = t5.chave::int
        AND t5.id_tabela = 'estabelecimento' AND t5.nome_coluna = 'tipo_prestador'
    LEFT JOIN raw.cnes__naturezas_juridicas n ON e.co_natureza_jur::int = n.id_natureza_juridica::int