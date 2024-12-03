-- Dados sobre os estabelecimentos provedores de cuidado do SUS de acordo com a metodologia do SHA (HP, Health Providers)
 MODEL (
    name sha.provedores,
    kind FULL
);


SELECT
   left(e.competencia, 4) as ano, -- Ano da competencia do registro do estabelecimento: YYYY (Fonte: CNES)
   right(e.competencia, 2) as mes, -- Mês da competencia do registro do estabelecimento: YYYY (Fonte: CNES)
   e.competencia, -- Ano e mês da competência do registro do estabelecimento: YYYYMM (Fonte: CNES)
   e.sigla_uf, -- Sigla da UF onde se localiza o estabelecimento (Fonte: CNES)
   e.id_municipio_ibge, -- Código IBGE de 6 digitos do município gestor do estabelecimento (Fonte: CNES)
   e.id_estabelecimento_cnes AS id_provedor, -- Código CNES do estabelecimento provedor de cuidados (Fonte: CNES)
   e.tipo_estabelecimento, -- Tipo de estabelecimento conforme CNES (ex. pronto socorro geral, hospital geral, centro de imunizacao etc) (Fonte: CNES)
   h.hp_code as codigo_sha, -- Código da função SHA correspondente ao tipo de estabelecimento (Fonte: NCS/DESID)
   e.codigo_natureza_juridica, -- Código da natureza juridica do estabelecimento (Fonte: Tabela de Naturezas Juridicas do CONCLA)
   COALESCE(
    CASE 
    WHEN n.descricao LIKE '%Federal%' THEN 'administracao publica federal'
    WHEN n.descricao LIKE '%Estadual%' THEN 'administracao publica estadual'
    WHEN n.descricao LIKE '%Municipal%' THEN 'administracao publica municipal'
    WHEN e.codigo_natureza_juridica IN ('1341')  THEN 'administracao publica federal' -- União
    WHEN e.codigo_natureza_juridica IN ('1244')  THEN 'administracao publica municipal' -- Município
    WHEN e.codigo_natureza_juridica BETWEEN '1000' and '1999' THEN 
        CASE
            WHEN trim(e.tipo_gestao) = 'municipal' THEN 'administracao publica municipal' -- Fundo, Comissão, Consórcio Municipal
            WHEN trim(e.tipo_gestao) = 'estadual' THEN 'administracao publica estadual' -- Fundo, Comissão, Consórcio Estadual
            WHEN right(e.id_municipio_ibge, 4) = '0000' THEN 'administracao publica estadual' -- Fundo, Comissão, Consórcio Estadual
            ELSE 'outra administracao publica'
        END
    WHEN e.codigo_natureza_juridica BETWEEN '2000' and '2999' THEN 'contratado' -- entidades empresariais
    WHEN e.codigo_natureza_juridica BETWEEN '3000' and '3999' THEN 'sem fins lucrativos' -- entidades sem fins lucrativos
    WHEN e.codigo_natureza_juridica BETWEEN '4000' and '4999' THEN 'contratado' -- pessoa física
    WHEN e.codigo_natureza_juridica BETWEEN '5000' and '5999' THEN 'instituicao extraterritorial' -- organização internacional e outras instituicoes extraterritoriais
    END,
    'desconhecido'
   ) AS tipo_provedor, -- Indicador to tipo de provedor: 'administracao publica federal', 'administracao publica estadual', 'administracao publica municipal', 'outra administracao publica', 'contratado', 'sem fins lucrativos', 'instituicao extraterritorial', 'desconhecido' (Fonte: CNES, NCD/DESID)
FROM
   cnes.estabelecimentos e
   LEFT JOIN raw.cnes__naturezas_juridicas n ON e.codigo_natureza_juridica::int = n.id_natureza_juridica::int
   LEFT JOIN raw.sha__tpups_hps h ON h.TPUPS::int = e.codigo_tipo_estabelecimento::int;

 /*  POST-STATEMENT */
@IF(
  @runtime_stage = 'evaluating',
  COPY sha.provedores
  TO 'data/outputs/br_ms__sha/provedores' WITH (
    FORMAT PARQUET,
    PARTITION_BY (ano, mes),
    OVERWRITE_OR_IGNORE,
    FILENAME_PATTERN 'provedores_')
  );
