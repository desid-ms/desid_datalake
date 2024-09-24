MODEL (
    name siops.receitas,
    audits (
        number_of_rows(threshold := 3000000),
        not_null(columns := (ano, fase, ente, codigo_ibge, sigla_uf, esfera, codigo_conta, descricao_conta, valor_receita)),
        accepted_values(column := ano, is_in=(2020, 2021, 2022)), -- ano é 2020, 2021 ou 2022
        accepted_range(column := valor_receita, min_v := 0, max_v := 1000000000000, inclusive := false, blocking:=false),
        -- iptu
        todos_municipios_tem_receita_imposto(blocking:=false, contas_imposto_like= '1.1.1.8.01.1%', threshold := 5, exceptions := (130006, 130010, 130020, 130130, 130395, 290020, 290323, 291250, 291535, 251335, 140040, 140050, 140070, 280200, 280250, 280530)),
        -- itbi
        todos_municipios_tem_receita_imposto(blocking:=false, contas_imposto_like= '1.1.1.8.01.4%', threshold := 5, exceptions := (130006, 130010, 130020, 130130, 130395, 290020, 290323, 291250, 291535, 251335, 140040, 140050, 140070, 280200, 280250, 280530)),
        -- iss
        todos_municipios_tem_receita_imposto(blocking:=false, contas_imposto_like= '1.1.1.8.02.3%', threshold := 5, exceptions := (130006, 130010, 130020, 130130, 130395, 290020, 290323, 291250, 291535, 251335, 140040, 140050, 140070, 280200, 280250, 280530)),
        -- cota parte ipva
        todos_municipios_tem_receita_imposto(blocking:=false, contas_imposto_like= '1.7.2.8.01.2.0', threshold := 5, exceptions := (-1)),

      
        
        

    ),
    kind FULL
);
-- 
        -- accepted_range(column := valor_receita, min_v := 0, max_v := 10**12, inclusive := false, blocking:=false)
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


AUDIT (
  name todos_municipios_tem_receita_imposto,
  dialect duckdb
);
  SELECT codigo_ibge, total_receita_realizada 
  FROM (
      SELECT codigo_ibge, SUM(valor_receita) AS total_receita_realizada
      FROM siops.receitas 
      WHERE fase = 'Receitas Realizadas Brutas' 
      AND esfera = 'Municipal' 
      AND codigo_conta LIKE @contas_imposto_like
      GROUP BY codigo_ibge
  )
  WHERE total_receita_realizada < @threshold 
  AND codigo_ibge not in @exceptions;



