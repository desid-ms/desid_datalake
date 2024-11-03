MODEL (
    name stg.sha__siops_despesas,
    kind FULL
);

WITH DESPESAS_CORRENTES AS (
    SELECT competencia, ibge, SUM(despesa_liquidada) AS valor 
    FROM siops.despesas
    WHERE 
        LEFT (conta, 2) = '3.' -- apenas despesas correntes
        AND fonte = 'Receitas Impostos e Transf.'
    GROUP BY 
        competencia, ibge
),
DESPESAS_NAO_CONSIDERADAS_APURACAO_MINIMO AS (
    SELECT competencia, ibge, SUM(despesa_liquidada) AS valor
    FROM siops.despesas
    WHERE
        conta IN (
            'ACDO000004', -- Inativos e pensionistas
            'ACDO000005' -- Despesas correntes com outras ações e serviços não computados
        )
    GROUP BY
        competencia, ibge
    ),
DESPESAS_COM_MEDICAMENTOS AS (
    SELECT competencia, ibge, SUM(despesa_liquidada) AS valor
    FROM siops.despesas
    WHERE
        conta IN ('3.3.30.30.01.00', '3.3.40.30.01.00', '3.3.90.30.09.00', '3.3.91.30.09.00')
    AND fonte = 'Receitas Impostos e Transf.'
    GROUP BY
        competencia, ibge
)
SELECT 
    left(correntes.competencia, 4) as competencia,
    correntes.ibge,
    correntes.valor as despesas_correntes,
    COALESCE(naoconsideradas.valor, 0) as despesas_nao_consideradas_apuracao_minimo,
    COALESCE(medicamentos.valor, 0) as despesas_medicamentos,
    (correntes.valor - COALESCE(naoconsideradas.valor, 0) - COALESCE(medicamentos.valor, 0)) as despesas_correntes_asps
FROM DESPESAS_CORRENTES correntes
LEFT JOIN DESPESAS_NAO_CONSIDERADAS_APURACAO_MINIMO naoconsideradas
    ON correntes.competencia = naoconsideradas.competencia
    AND correntes.ibge = naoconsideradas.ibge
LEFT JOIN DESPESAS_COM_MEDICAMENTOS medicamentos
    ON correntes.competencia = medicamentos.competencia
    AND correntes.ibge = medicamentos.ibge


