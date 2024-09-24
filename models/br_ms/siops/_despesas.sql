MODEL (
    name siops.despesas,
    audits(
        number_of_rows(threshold := 8000000),
        not_null(columns := (ano, fase, ente, codigo_ibge, sigla_uf, esfera, codigo_conta, descricao_conta, fonte_recursos, subfuncao, valor_despesa)),
        accepted_values(column := ano, is_in=(2020, 2021, 2022)), -- ano é 2020, 2021 ou 2022
        accepted_range(column := valor_despesa, min_v := 0, max_v := 1000000000000, inclusive := false, blocking:=false),
        
    ),
    kind FULL
);

select distinct   
    ano, -- Ano competência da Despesa
    TRIM(REGEXP_REPLACE(fase, '\s+\([a-z]\)', '', 'gi')) AS fase, -- Fase da Despesa (Dotação Atualizada, Despesa Empenhada, Despesa Liquidada, Despesa Paga)
    ente, -- Município ou Estado
    IBGE as codigo_ibge, -- Código IBGE do Ente
    UF as sigla_uf, -- Sigla da Unidade da Federação do Ente
    ESFERA as esfera, -- Esfera de governo do ente (Municipal, Estadual)
    CODIGO as codigo_conta, -- Código da conta de despesa no SIOPS
    DESCRICAO as descricao_conta, -- Descrição da conta de despesa no SIOPS
    REPLACE(PASTA, 'Fonte - ', '') as fonte_recursos, -- Fonte dos recursos para despesa
    REPLACE(SUBFUNCAO, 'Subfunção - ', '') as subfuncao, -- Subfunção de saúde destino da despesa
    CAST(REPLACE(VALOR, ',', '.') AS DECIMAL(14,2)) AS valor_despesa -- Valor da despesa no período de competência
from
  read_csv_auto('data/inputs/**/*/Desp*.txt', delim = ';', header = true,
  hive_partitioning=true);



--   AUDIT (
--   name saldo_despesas_positivo,
--   dialect duckdb
-- );
-- select *
-- from (
--         SELECT 
--             ano,
--             ente,
--             codigo_ibge,
--             codigo_conta,
--             fonte_recursos,
--             subfuncao,
--             SUM(
--                 CASE
--                     WHEN fase = 'Receitas Orçadas' THEN valor_despesa
--                     ELSE 0
--                 END
--             ) AS Receitas_Orcadas,
--             SUM(
--                 CASE
--                     WHEN fase = 'Receitas Realizadas Brutas' THEN valor_despesa
--                     ELSE 0
--                 END
--             ) AS Receitas_Realizadas,
--             SUM(
--                 CASE
--                     WHEN fase = 'Despesas Empenhadas' THEN valor_despesa
--                     ELSE 0
--                 END
--             ) AS Despesas_Empenhadas,
--             SUM(
--                 CASE
--                     WHEN fase = 'Despesas Liquidadas' THEN valor_despesa
--                     ELSE 0
--                 END
--             ) AS Despesas_Liquidadas,
--             SUM(
--                 CASE
--                     WHEN fase = 'Despesas Pagas' THEN valor_despesa
--                     ELSE 0
--                 END
--             ) AS Despesas_Pagas,
--             (Despesas_Empenhadas - Despesas_Liquidadas) AS Saldo_Empenho,
--             (Despesas_Liquidadas - Despesas_Pagas) AS Saldo_Liquidado
--         FROM siops.despesas
--         GROUP BY all
--     )
-- WHERE 
--     codigo_conta !='ACDO000002'
--     AND
--     (Saldo_Empenho < 0
--     OR Saldo_Liquidado < 0 )