MODEL (
   name sia.procedimentos, 
   kind FULL
);

SELECT
   last (dt_cmp) AS competencia,
   lPAD ((CO_PA_ID || CO_PA_DV), 10, '0') AS codigo_procedimento,
   last (ds_pa_dc) AS procedimento,
   last (nu_pa_total) AS valor
FROM
   raw.sia__procedimentos
GROUP BY
   co_pa_id,
   co_pa_dv;