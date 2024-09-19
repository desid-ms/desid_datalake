MODEL (
    name siops.contas,
    kind SEED
);
WITH contas AS (
    SELECT 
        conta AS id_conta,
        ordem_conta,
        parent AS codigo_conta_mae, 
        ds_conta as descricao_conta,
        FROM read_parquet('data/inputs/contas/contas.parquet')
)
SELECT 
    id_conta, -- Código da conta no SIOPS
    ordem_conta, -- Ordem da conta no SIOPS
    descricao_conta, -- Descricao da conta
    codigo_conta_mae, -- Código da conta mãe que agrega o valor desta conta
    (
        SELECT COUNT(*) = 0
        FROM contas c
        WHERE c.codigo_conta_mae = contas.id_conta
    ) AS indicador_folha -- Código que indica que essa conta não é mãe de nenhuma outra
FROM contas;