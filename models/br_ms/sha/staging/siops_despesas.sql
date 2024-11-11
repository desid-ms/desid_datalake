MODEL (
    name stg.sha__siops_despesas,
    kind FULL
);

WITH DESPESAS_CORRENTES AS (
    SELECT competencia, ibge, fonte, destinacao, SUM(despesa_liquidada) AS valor 
    FROM siops.despesas
    WHERE 
        LEFT (conta, 2) = '3.' -- apenas despesas correntes
        AND fonte = 'Receitas Impostos e Transf.'
    GROUP BY ALL
),
DESPESAS_NAO_CONSIDERADAS_APURACAO_MINIMO AS (
    SELECT competencia, ibge, fonte, destinacao, SUM(despesa_liquidada) AS valor
    FROM siops.despesas
    WHERE
        fonte = 'Receitas Impostos e Transf.'
    AND conta IN ( 
            '3.1.90.01.00.00', --   Aposentadorias do RPPS, Reserva Remunerada e Reforma dos Militares
            '3.1.90.03.00.00', --   Pensões do RPPS e do Militar
            '3.1.90.05.00.00', --	Outros Benefícios Previdenciários do Servidor ou do Militar
            '3.1.90.92.01.00', --	Aposentadorias, Reserva Remunerada e Reformas dos Militares
            '3.1.90.92.03.00', --	Pensões do RPPS e do Millitar
            '3.1.90.92.05.00', --	Outros Benefícios Previdenciários do Servidor ou do Militar
            '3.1.90.94.03.00', --	Indenizações e Restituições Trabalhistas - Inativo Civil
            '3.1.90.94.04.00', --	Indenizações e Restituições Trabalhistas - Inativo Militar
            '3.1.90.94.06.00', --	Indenizações e Restituições Trabalhistas - Pensionista Militar
            '3.1.90.94.13.00', --	Indenizações e Restituições Trabalhistas - Pensionista Civil
            -- As contas 3.1.90.* acima totalizam na ACDO000004
            'ACDO000005' -- Despesas correntes com outras ações e serviços não computados
        )
    GROUP BY ALL
    ),
DESPESAS_COM_MEDICAMENTOS AS (
    SELECT competencia, ibge, fonte, destinacao, SUM(despesa_liquidada) AS valor
    FROM siops.despesas
    WHERE
        conta IN ('3.3.30.30.01.00', '3.3.40.30.01.00', '3.3.90.30.09.01', '3.3.90.30.09.02', '3.3.91.30.09.00')
    AND fonte = 'Receitas Impostos e Transf.'
    GROUP BY ALL
)
SELECT 
    left(correntes.competencia, 4) as competencia,
    correntes.ibge,
    correntes.fonte, 
    correntes.destinacao,
    correntes.valor as despesas_correntes,
    COALESCE(naoconsideradas.valor, 0) as despesas_nao_consideradas_apuracao_minimo,
    COALESCE(medicamentos.valor, 0) as despesas_medicamentos,
    (correntes.valor - COALESCE(naoconsideradas.valor, 0) - COALESCE(medicamentos.valor, 0)) as despesas_correntes_asps
FROM DESPESAS_CORRENTES correntes
LEFT JOIN DESPESAS_NAO_CONSIDERADAS_APURACAO_MINIMO naoconsideradas
    ON correntes.competencia = naoconsideradas.competencia
    AND correntes.ibge = naoconsideradas.ibge
    AND correntes.fonte = naoconsideradas.fonte
    AND correntes.destinacao = naoconsideradas.destinacao
LEFT JOIN DESPESAS_COM_MEDICAMENTOS medicamentos
    ON correntes.competencia = medicamentos.competencia
    AND correntes.ibge = medicamentos.ibge
    AND correntes.fonte = medicamentos.fonte
    AND correntes.destinacao = medicamentos.destinacao;


