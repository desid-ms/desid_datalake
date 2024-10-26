MODEL (
    name sih.producao_hospitalar,
    kind FULL
);

    WITH BASE AS (
        SELECT LEFT(dt_saida, 6) as competencia, * FROM raw.sih__tb_reduz_rejeit
        WHERE LEFT(dt_saida, 4) = '2022'
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
            * EXCLUDE(nu_val_tot),
            LAST(nu_val_tot) as nu_val_tot
        FROM (SELECT * FROM REDUZIDOS UNION SELECT * FROM REJEITADOS) 
        GROUP BY ST_SITUACAO, CO_IDENT, CO_CNES, NU_ESPECIALIDADE, NU_AIH, CO_PROC_RELIZADO, DT_INTERNACAO
        ORDER BY dt_saida, dt_cmpt
    ),
    PRODUCAO_DETALHADA AS (
        SELECT
            p.competencia,
            co_cnes AS cnes_estabelecimento,
            e.codigo_sha as codigo_sha_estabelecimento_competencia,
            nu_especialidade AS codigo_especialidade_leito,
            t2.valor AS especialidade_leito, --pegar do cnv ou do chat
            co_proc_realizado AS codigo_procedimento,
            proc.procedimento_sus as procedimento,
            proc.codigo_sha AS codigo_sha_procedimento,
            nu_aih AS aih,
            last(st_situacao) AS indicador_rejeitado,
            CASE co_ident
                WHEN '1' THEN 'CURTA'
                WHEN '5' THEN 'LONGA'
                ELSE NULL
            END AS permanencia,
            dt_internacao AS data_internacao,
            last(dt_saida) AS data_saida,
            -- last(dt_cmpt)
            co_financiamento AS codigo_tipo_financiamento,
            coalesce(last(t1.valor), 'media e alta complexidade - mac') as tipo_financiamento,
            sum(nu_val_tot::DECIMAL) AS valor
        FROM (SELECT * FROM REDUZIDOS UNION SELECT * FROM REJEITADOS) p
        LEFT JOIN raw.sha__tipos t1
            ON t1.chave::int = co_financiamento::int
            AND t1.id_tabela = 'producao_ambulatorial'
            AND t1.nome_coluna = 'tipo_financiamento_producao'
        LEFT JOIN raw.cnes__tipos t2
            ON t2.chave::int = nu_especialidade::int
            AND t2.id_tabela = 'leito'
            AND t2.nome_coluna = 'tipo_especialidade_leito'
        LEFT JOIN sha.procedimentos proc 
            ON co_proc_realizado::int = proc.id_procedimento_sigtap::int
        LEFT JOIN sha.provedores e 
            ON e.id_provedor::int = co_cnes::int 
            AND e.competencia::int = p.competencia::int
        GROUP BY ALL
    )
    SELECT 
        competencia, -- ano e mês da internação (longa permanência considera o último)
        cnes_estabelecimento, -- código cnes do estabelecimento provedor da internação
        codigo_sha_estabelecimento_competencia,  -- código sha do estabelecimento no momento da competência
        codigo_especialidade_leito,  -- código da especialidade do leito no cnes
        especialidade_leito, -- especialidade do leito usado na internação
        codigo_procedimento,  -- procedimento 
        codigo_sha_procedimento, 
        procedimento, 
        tipo_financiamento, 
        count(aih) as quantidade_internacoes, 
        sum(valor) as valor_total 
    FROM producao_detalhada 
    GROUP BY ALL 
    ORDER BY ALL;

--select st_situacao, co_ident, co_cnes, nu_especialidade, co_proc_realizado, nu_aih, dt_internacao, dt_saida, nu_val_tot from rejeitados where nu_aih='5222102289426' order by dt_saida;
-- tem procedimentos diferentes e está pegando apenas um

--select st_situacao, co_ident, co_cnes, nu_especialidade, co_proc_realizado, nu_aih, dt_internacao, dt_saida, nu_val_tot from rejeitados where nu_aih ='9922300003407' and co_proc_realizado = '0415010012' and dt_saida = '20220618';
-- qual deduplica
