MODEL (
    name siconfi.entes,
    kind FULL,
    grain cnpj
);


select distinct
  cod_ibge::UINTEGER as codigo_ibge,
  ente::STRING,
  capital::STRING,
  regiao::STRING,
  uf::STRING,
  esfera::STRING,
  exercicio::UINTEGER,
  populacao::UINTEGER,
  cnpj::STRING,
from 
  read_json_auto('data/inputs/entes/**/*.json',
  format='array', 
  hive_partitioning=false);