MODEL (
  name raw.sha__HC_CAES,
  kind FULL
);

SELECT 
    codigo_sigtap, 
    nome_procedimento_2022, 
    codigo_SHA 
FROM 
    ST_READ('data/inputs/sha/Planilha_SIGTAP_SHA_areas_tecnicas.xlsx') 