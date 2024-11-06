MODEL (
    name raw.sisab__es_atendimentos,
    kind FULL
);
    
 WITH expanded_json AS (
      SELECT
          ID_ATENDIMENTO,
          UNNEST(CASE
              WHEN TRY_CAST(DS_ATENDIMENTO_JSON AS JSON) IS NOT NULL
              AND DS_ATENDIMENTO_JSON LIKE '[%'
              THEN json(DS_ATENDIMENTO_JSON)::JSON[]
              END) AS record
      FROM
          raw.sisab__es
  )
  SELECT
      ID_ATENDIMENTO,
      json_keys(record)[1] as key,
      TRIM('"' FROM record[json_keys(record)[1]]::VARCHAR) as value
  FROM expanded_json
  WHERE record IS NOT NULL;