MODEL (
    name sha.fatores,
    kind FULL
);

-- No barramento do SISAB essa é a View chamada CGES
select distinct   
    * 
from ST_READ('data/inputs/sha/valor_PAB_2020_2022.xlsx')
  