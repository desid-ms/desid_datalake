MODEL (
   name sigtap.procedimentos,
   kind FULL
);
SELECT
    p.DT_COMPETENCIA AS competencia,
    p.CO_PROCEDIMENTO AS codigo_procedimento,
    TRIM(p.NO_PROCEDIMENTO) AS procedimento,
    r.CO_CID AS codigo_cid,
    TRIM(c.NO_CID) AS doenca_tratada,
    p.CO_FINANCIAMENTO AS codigo_tipo_financiamento
FROM
    raw.sigtap__tb_procedimentos p
    LEFT JOIN raw.sigtap__rl_procedimento_cid r ON p.CO_PROCEDIMENTO = r.CO_PROCEDIMENTO
    LEFT JOIN raw.sigtap__tb_cid c ON c.CO_CID = r.CO_CID