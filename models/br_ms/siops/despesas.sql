MODEL (
    name siops.despesas,
    kind FULL
);

select distinct   
    ano, -- Ano competência da Despesa
    TRIM(REGEXP_REPLACE(fase, '\s+\([a-z]\)', '', 'gi')) AS fase, -- Fase da Despesa (Orçada, Empenhada, Liquidada, Paga)
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