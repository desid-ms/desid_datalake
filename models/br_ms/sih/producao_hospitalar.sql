MODEL (
    name sih.producao,
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
        dt_saida, (LAST(QT_DIAS_PERM)) as qt_dias_perm, LAST(dt_cmpt) as dt_cmpt, CO_FINANCIAMENTO, LAST(nu_val_tot) as nu_val_tot
    FROM (SELECT * FROM REDUZIDOS UNION SELECT * FROM REJEITADOS)
    GROUP BY competencia, ST_SITUACAO, CO_IDENT, CO_CNES, NU_ESPECIALIDADE, NU_AIH, CO_PROC_REALIZADO, DT_INTERNACAO, DT_SAIDA, CO_FINANCIAMENTO
    ORDER BY dt_saida, dt_cmpt
),
AGREGADOS AS (
    SELECT
        competencia,
        co_cnes::int AS cnes_provedor,
        nu_aih AS aih,
        co_proc_realizado::int AS codigo_procedimento_principal,
        CASE co_ident
            WHEN '1' THEN 'curta'
            WHEN '5' THEN 'longa'
            ELSE NULL
        END AS permanencia,
        strptime(dt_internacao, '%Y%m%d')::date AS data_internacao,
        strptime(last(dt_saida), '%Y%m%d')::date AS data_saida,
        last(dt_cmpt) AS competencia_pagamento,
        sum(nu_val_tot::DECIMAL) AS valor_internacao
    FROM DEDUPLICADOS d
    GROUP BY ALL
)
SELECT
    competencia, -- ano e mês do fim da internacao
    cnes_provedor, -- código do estabelecimento onde ocorreu internação no CNES (Cadastro Nacional de Estabelecimentos de Saúde)
    aih, -- código identificador da internação no SIH (inclui "autorizações rejeitadas")
    codigo_procedimento_principal, -- código do procedimento principal que motivou a internação no SIGTAP
    permanencia, -- tipo de permanência (curta ou longa)
    data_internacao, -- data de entrada do paciente, início da internação
    data_saida, -- data de saída do paciente, fim da internação 
    (data_saida - data_internacao + 1) as dias_permanencia, -- dias de internação
    competencia_pagamento, -- mês e ano quando se processou o pagamento da aih no sih
    valor_internacao -- valor no sih referente à internação 
FROM AGREGADOS;