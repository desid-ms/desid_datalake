MODEL (
    name siconfi.entes,
    kind FULL,
    grain cnpj
);

select distinct
  
  cod_ibge::text as codigo_ibge_7,
  left(cod_ibge::text, 6) as codigo_ibge_6,
  ente::STRING,
  capital::STRING,
  regiao::STRING,
  uf::STRING,
  esfera::STRING,
  exercicio::UINTEGER,
  populacao::UINTEGER,
  cnpj::STRING,
from 
  read_json_auto('data/inputs/siconfi/entes/**/*.json',
  format='array', 
  hive_partitioning=false);