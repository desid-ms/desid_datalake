MODEL (
    name sisab.cges,
    kind FULL
);

-- No barramento do SISAB essa é a View chamada CGES
select distinct   
    * 
from
  read_csv_auto('data/inputs/sisab/VW_SISAB_CGES_202408071251.csv', delim = ';', header = true);