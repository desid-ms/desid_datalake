MODEL (
    name stg.sih__producao,
    kind FULL
);
    
WITH BASE AS (
    SELECT LEFT(dt_saida, 6) as competencia, * FROM raw.sih__tb_reduz_rejeit
    -- para co_ident = 1 --> curta --> competencia é dt_internacao // conferir com Danilo
),
REDUZIDOS AS (
    SELECT * FROM BASE WHERE ST_SITUACAO = '0'
),
REJEITADOS AS (
    SELECT * FROM BASE
    WHERE ST_SITUACAO = '1'
    AND nu_aih NOT IN (SELECT nu_aih FROM REDUZIDOS)
),
DEDUPLICADOS AS (
    SELECT
        competencia, ST_SITUACAO, CO_IDENT, CO_CNES, NU_ESPECIALIDADE, NU_AIH, CO_PROC_REALIZADO, DT_INTERNACAO,
        dt_saida, LAST(QT_DIAS_PERM) as qt_dias_perm, LAST(dt_cmpt) as dt_cmpt, CO_FINANCIAMENTO, LAST(nu_val_tot) as nu_val_tot
    FROM (SELECT *  FROM REDUZIDOS UNION SELECT * FROM REJEITADOS)
    GROUP BY competencia, ST_SITUACAO, CO_IDENT, CO_CNES, NU_ESPECIALIDADE, NU_AIH, CO_PROC_REALIZADO, DT_INTERNACAO, DT_SAIDA, CO_FINANCIAMENTO
    ORDER BY dt_saida, dt_cmpt
)
    SELECT
        d.competencia as competencia_internacao, -- ano e mês do cuidado prestado
        co_cnes::int AS cnes_provedor, -- código CNES do estabelecimento provedor do cuidado
        nu_especialidade::int AS codigo_especialidade_leito, -- código da especialidade do leito
        co_proc_realizado::int AS codigo_procedimento_principal, -- principal procedimento realizado durante internação
        nu_aih AS aih, -- código da aprovação de internacação hospitalar
        CASE co_ident
            WHEN '1' THEN 'curta'
            WHEN '5' THEN 'longa'
            ELSE NULL
        END AS permanencia, -- tipo de internação: curta permanência (<=45 dias) ou longa permanência (>45 dias)
        dt_internacao AS data_internacao, -- data da internação, ocupação do leito
        last(dt_saida) AS data_saida, -- data da saída, desocupação do leito
        last(dt_cmpt) AS competencia_pagamento, -- ano e mês de competencia da aih, refere-se ao momento da autorização ou rejeição de pagamento
        co_financiamento::int as codigo_tipo_financiamento,
        sum(qt_dias_perm) AS quantidade_dias_permanencia,
        sum(nu_val_tot::DECIMAL) AS valor_total -- valor total da internação
    FROM DEDUPLICADOS d
    GROUP BY ALL
   