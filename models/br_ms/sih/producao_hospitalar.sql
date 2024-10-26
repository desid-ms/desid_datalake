MODEL (
    name sih.producao_hospitalar,
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
    prov.codigo_sha as codigo_sha_provedor_competencia, -- codigo sha do estabelecimento provedor do cuidado no momento da competencia
    prov.tipo_provedor as tipo_provedor_competencia, -- tipo do provedor (publico federal, contratado, sem fins lucrativos etc)
    nu_especialidade::int AS codigo_especialidade_leito, -- código da especialidade do leito
    e.descricao as especialidade_leito,
    co_proc_realizado::int AS codigo_procedimento_principal, -- principal procedimento realizado durante internação
    COALESCE(e.codigo_sha, proc.codigo_sha) as HC, -- código sha da especialidade_leito ou procedimento (cuidado prestado)
    nu_aih AS aih, -- código da aprovação de internacação hospitalar
    CASE co_ident
        WHEN '1' THEN 'curta'
        WHEN '5' THEN 'longa'
        ELSE NULL
    END AS permanencia, -- tipo de internação: curta permanência (<=45 dias) ou longa permanência (>45 dias)
    dt_internacao AS data_internacao, -- data da internação, ocupação do leito
    last(dt_saida) AS data_saida, -- data da saída, desocupação do leito
    last(dt_cmpt) AS competencia_pagamento, -- ano e mês de competencia da aih, refere-se ao momento da autorização ou rejeição de pagamento
    co_financiamento::int as codigo_tipo_financiamento, -- código do tipo de financiamento
    t1.valor as tipo_financiamento, -- tipo de financiamento
    sum(qt_dias_perm) AS quantidade_dias_permanencia, -- duração da internação em dias
    sum(nu_val_tot::DECIMAL) AS valor_total -- valor nominal total da internação 
FROM DEDUPLICADOS d
LEFT join read_csv('data/inputs/sha/sih_especialidade.csv') e on e.codigo = nu_especialidade
LEFT JOIN
   sha.procedimentos proc ON d.co_proc_realizado::int = proc.codigo_procedimento::int
LEFT JOIN
   sha.provedores prov ON prov.id_provedor::int = d.co_cnes::int AND prov.competencia::int = d.competencia::int
LEFT JOIN raw.sha__tipos t1 ON d.co_financiamento::int = t1.chave::int
        AND t1.id_tabela = 'producao_ambulatorial' AND t1.nome_coluna = 'tipo_financiamento_producao' -- mesmo que sia
GROUP BY ALL



