MODEL (
   name raw.sigtap__tb_cid,
   kind FULL
);

SELECT
   SUBSTRING(COL1, 1, 4) as CO_CID,
   SUBSTRING(COL1, 5, 100) as NO_CID,
   SUBSTRING(COL1, 105, 1) as TP_AGRAVO,
   SUBSTRING(COL1, 106, 1) as TP_SEXO,
   SUBSTRING(COL1, 107, 1) as TP_ESTADIO,
   CAST(SUBSTRING(COL1, 108, 4) AS INTEGER) as VL_CAMPOS_IRRADIADOS
FROM read_csv(
   'data/inputs/sha/SIGTAP/tb_cid.txt',
   header = false,
   columns = {'COL1':'TEXT'}
);