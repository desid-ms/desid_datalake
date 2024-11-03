MODEL (
    name raw.sisab__es_atendimentos,
    kind FULL
);
    
WITH expanded_json AS (
    SELECT
        ID_ATENDIMENTO,
        UNNEST(json(DS_ATENDIMENTO_JSON)::JSON[]) AS record
    FROM
        raw.sisab__es
)
SELECT 
    ID_ATENDIMENTO,
    json_keys(record)[1] as key,
    TRIM('"' FROM record[json_keys(record)[1]]::VARCHAR) as value
FROM expanded_json;
