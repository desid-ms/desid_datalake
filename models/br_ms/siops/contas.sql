-- Plano de contas do SIOPS no período de competência
MODEL (
  name siops.contas,
  kind FULL
);

select 
  competencia, -- Ano competência do plano de contas YYYY
  codigo_conta, -- Código da conta no formato X.X.X.X.XX.X.X ou ACDOXXXXXX ou ACROXXXXXX
  descricao_conta, -- Descrição da conta
  tipo_conta, -- Indicador se conta é do tipo agregadora (calculada) ou operacional (informada pelo ente)
  ativo_estado, -- Indicador se conta pode ser usada para ente Estadual 'S' ou não 'N'
  ativo_df, -- Indicador se conta pode ser usada para ente Distrito Federal 'S' ou não 'N'
  ativo_municipio -- Indicador se conta pode ser usada para ente Municipal 'S' ou não 'N'

from raw.siops__contas
-- --  POST-STATEMENT
-- @IF(
--   @runtime_stage = 'evaluating',
--   COPY siops.receitas TO 'data/outputs/siops__contas' 
--     (FORMAT PARQUET, PARTITION_BY (competencia), OVERWRITE, FILENAME_PATTERN 'siops__contas_{uuid}')
-- );
