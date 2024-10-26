MODEL (
   name raw.sigtap__rl_procedimento_cid,
   kind FULL
);

SELECT
   SUBSTRING(COL1, 1, 10) as CO_PROCEDIMENTO,
   SUBSTRING(COL1, 11, 4) as CO_CID,
   SUBSTRING(COL1, 15, 1) as ST_PRINCIPAL, 
   SUBSTRING(COL1, 16, 6) as DT_COMPETENCIA
FROM read_csv(
   'data/inputs/sha/SIGTAP/rl_procedimento_cid.txt',
   header = false,
   columns = {'COL1':'TEXT'}
);