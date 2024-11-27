-- Produção Hospitalar  a partir dos registros de autorização de internação hospitalar registrados no SIH
MODEL (
    name sih.producao,
    kind FULL
);

WITH BASE AS (
    SELECT LEFT(dt_saida, 6) as competencia, * FROM raw.sih__tb_reduz_rejeit
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
        co_cnes::int AS id_estabelecimento_cnes,
        nu_aih AS id_aih,
        CASE ST_SITUACAO
            WHEN '0' THEN 'reduzida'
            WHEN '1' THEN 'rejeitada'
            ELSE NULL
        END AS situacao,
        co_proc_realizado::int AS procedimento_principal,
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
    competencia, -- Ano e mês do fim da internacao: YYYYMM
    competencia_pagamento, -- Mês e ano do processamento do pagamento da aih no sih: YYYYMM
    id_estabelecimento_cnes, -- Código do estabelecimento onde ocorreu internação no CNES (Cadastro Nacional de Estabelecimentos de Saúde)
    id_aih, -- Código identificador da autorização de internação no SIH (inclui "autorizações rejeitadas" deduplicadas que não estão também entre "autorizações reduzidas")
    situacao, -- Situação da AIH: reduzida (autorizada) ou rejeitada
    procedimento_principal, -- Código do procedimento principal que motivou a internação no SIGTAP
    permanencia, -- Tipo de permanência (curta ou longa)
    data_internacao, -- Data de entrada do paciente, início da internação
    data_saida, -- Data de saída do paciente, fim da internação 
    (data_saida - data_internacao + 1) as dias_permanencia, -- Dias de internação (data_saida - data_internacao + 1)
    valor_internacao, -- Valor nominal em Reais no sih referente à internação 
FROM AGREGADOS;