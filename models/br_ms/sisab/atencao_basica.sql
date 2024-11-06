MODEL (
    name sisab.producao,
    kind FULL
);
    
WITH BASE AS (
    SELECT
        ID_ATENDIMENTO,
        COMPETENCIA,
        CO_TIPO_FICHA_ATENDIMENTO,
        CO_UF_IBGE,
        CO_MUNICIPIO_IBGE,
        CO_CNES,
        TP_EQUIPE,
        CO_LOCAL_ATENDIMENTO,
        DS_ATENDIMENTO,
        DS_ATENDIMENTO_JSON,
        CO_TIPO_ATENDIMENTO,
        CO_CBO_OCUPACAO
    FROM raw.sisab__es
)
    select * from BASE
