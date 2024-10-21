MODEL (
    name siconfi.entes,
    kind FULL,
    grain cnpj
);

select distinct
  
  cod_ibge::text as codigo_ibge_7,
  left(cod_ibge::text, 6) as codigo_ibge_6,
  ente::STRING as ENTE,
  capital::STRING as capital,
  case regiao::STRING 
  when 'NO' THEN 'N'
  when 'SU' THEN 'S'
  else regiao::STRING
  END as regiao 
  ,
  uf::STRING as UF,
  esfera::STRING as esfera,
  exercicio::UINTEGER as exercicio,
  populacao::UINTEGER as populacao,
  cnpj::STRING as cnpj,
from 
  read_json_auto('data/inputs/siconfi/entes/**/*.json',
  format='array', 
  hive_partitioning=false);