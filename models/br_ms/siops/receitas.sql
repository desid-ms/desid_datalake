MODEL (
    name siops.receitas,
    kind FULL
);

select distinct   
    ano, -- Ano competência da Receita
    TRIM(REGEXP_REPLACE(fase, '\s+\([a-z]\)', '', 'gi')) AS fase,  -- Fase da Receita 
    ENTE as ente, -- Município ou Estado
    IBGE as codigo_ibge, -- Código IBGE do Ente
    UF as sigla_uf,  -- Sigla da Unidade da Federação do Ente
    ESFERA as esfera, -- Esfera de governo do ente (Municipal, Estadual)
    CODIGO as codigo_conta, -- Código da conta de receita no SIOPS
    DESCRICAO as descricao_conta, -- Código da conta de receita no SIOPS
    CAST(REPLACE(VALOR, ',', '.') AS DECIMAL(14,2)) AS valor_receita -- Valor da receita no período de competência
from
  read_csv_auto('data/inputs/**/*/Recei*.txt', delim = ';', header = true,
  hive_partitioning=true);