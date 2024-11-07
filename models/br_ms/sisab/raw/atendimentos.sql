MODEL (
    name raw.sisab__es_atendimentos,
    kind FULL
);

/*
Objetivo: Esta query transforma uma string de pares chave-valor separados por vírgula da coluna DS_ATENDIMENTO 
em linhas separadas com colunas estruturadas para chave, valor e quantidade.

Exemplo de formato de entrada: ",PRC=0414020138=04,CDT=16,VSB=99,CSO=2,"
Exemplo de formato de saída:
ID_ATENDIMENTO | key | value      | quantity
--------------+-----+------------+----------
12725783181   | PRC | 0414020138 | 04
12725783181   | CDT | 16         | 01
12725783181   | VSB | 99         | 01
12725783181   | CSO | 2          | 01
*/

-- Primeira CTE: Divide a string separada por vírgulas em itens individuais
WITH split_values AS (
    SELECT
        ID_ATENDIMENTO,
        -- STRING_TO_ARRAY divide a string nas vírgulas
        -- UNNEST converte o array resultante em linhas
        -- TRIM remove vírgulas do início e fim de cada item
        TRIM(',' FROM UNNEST(STRING_TO_ARRAY(DS_ATENDIMENTO, ',')) ) as item
    FROM raw.sisab__es
    WHERE DS_ATENDIMENTO IS NOT NULL AND DS_ATENDIMENTO != ''
),

-- Segunda CTE: Analisa cada item em componentes de chave, valor e quantidade
parsed_values AS (
    SELECT
        ID_ATENDIMENTO,
        -- Extrai a chave (texto antes do primeiro '=')
        SPLIT_PART(item, '=', 1) as key,
        -- Extrai o valor
        NULLIF(TRIM('=' FROM SPLIT_PART(item, '=', 2)), '') as value,
        
        -- ALguns PRC tem quantidade depois, outros não
        -- Extrai a quantidade
        -- Para PRC: usa a terceira parte após '='
        -- Para todos os outros: padrão '01'
        CASE
            WHEN key = 'PRC' THEN NULLIF(SPLIT_PART(item, '=', 3), '')
            ELSE '01'
        END as quantity
    FROM split_values
    -- Filtra itens vazios
    WHERE item != ''
)
SELECT
    ID_ATENDIMENTO,
    key,
    value,
    SUM(COALESCE(TRY_CAST(quantity AS INTEGER), 1)) as quantity
FROM parsed_values
WHERE key != ''
GROUP BY ID_ATENDIMENTO, key, value;
