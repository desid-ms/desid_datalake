-- Plano de contas do SIOPS no período de competência
MODEL (
  name siops.contas,
  kind FULL
);

SELECT
  competencia as ano, -- Ano competência do plano de contas YYYY
  codigo_conta_siops, -- Identificador da conta no sistema SIOPS
  codigo_conta, -- Código da conta no formato X.X.X.X.XX.X.X ou ACDOXXXXXX ou ACROXXXXXX
  descricao_conta, -- Descrição da conta
  tipo_conta, -- Indicador se conta é do tipo agregadora (calculada) ou operacional (informada pelo ente)
  ativo_estado, -- Indicador se conta pode ser usada para ente Estadual: S ou N
  ativo_df, -- Indicador se conta pode ser usada para ente Distrito Federal: S ou N
  ativo_municipio -- Indicador se conta pode ser usada para ente Municipal: S ou N
FROM
  raw.siops__contas;

 /*  POST-STATEMENT */
@IF(
  @runtime_stage = 'evaluating',
COPY siops.contas TO 'data/outputs/br_ms__siops' WITH
    (FORMAT PARQUET, PARTITION_BY (ano), OVERWRITE, FILENAME_PATTERN 'contas')
);