-- Divisão Territorial Brasileira apresenta a relação de regioes, mesorregioes, unidades federativas, microrregioes e municípios que compõem a divisão territorial brasileira oficial segundo o IBGE
MODEL (
  name dtb.territorio,
  kind FULL
);

SELECT
  codigo_regiao::INTEGER as id_regiao, -- código ibge para as regioes brasileiras: 1=Norte;2=Nordeste;3=Sudeste;4=Sul;5=Centro-Oeste
  nome_regiao, -- nome da região: Norte, Nordeste, Sudeste, Sul, Centro-Oeste 
  codigo_uf::INTEGER as id_uf, -- código do ibge com dois digitos que representam unicamente cada unidade da federação 
  sigla_uf::VARCHAR, -- sigla da unidade da federação 
  nome_uf::VARCHAR, -- nome da unidade da federação 
  codigo_regiao_geografica_intermediaria::INTEGER as id_regiao_geografica_intermediaria, -- código ibge que identifica unicamente cada região geográfica intermediária 
  regiao_geografica_intermediaria::VARCHAR, -- região geográfica intermediária 
  codigo_regiao_geografica_imediata::INTEGER as id_regiao_geografica_imediata, -- código ibge que identifica unicamente cada região geográfica imediata 
  regiao_geografica_imediata::VARCHAR, -- região geográfica imediata 
  codigo_mesorregiao::INTEGER as id_mesorregiao, --  código ibge que identifica unicamente cada mesorregião 
  nome_mesoregiao::VARCHAR, -- nome da mesorregião 
  codigo_microrregiao::INTEGER as id_microrregiao, -- código ibge que identifica unicamente cada microrregião 
  nome_microrregiao::VARCHAR, -- nome da microrregião 
  codigo_municipio_5::INTEGER as id_municipio_5, -- código ibge que identifica unicamente cada município com 5 dígitos 
  codigo_municipio_6::INTEGER as id_municipio_6, -- código ibge que identifica unicamente cada município com 6 dígitos 
  codigo_municipio_7::INTEGER as id_municipio_7, -- código ibge que identifica unicamente cada município com 7 dígitos 
  nome_municipio::VARCHAR -- nome oficial do município 
FROM READ_PARQUET('data/inputs/ibge.parquet')