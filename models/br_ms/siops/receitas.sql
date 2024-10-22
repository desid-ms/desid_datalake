MODEL (
    name siops.receitas,
    audits (
        todos_municipios_tem_receita_IPTU,
        todos_municipios_tem_receita_ITBI,  
        todos_municipios_tem_receita_ISS,
        todos_municipios_tem_receita_CotaParteIPVA,
        todos_estados_tem_receita_IPVA_acima_minimo(minimo:=5000000),
        todos_estados_tem_receita_ITCMD_acima_minimo(minimo:=5000000),
        
    )
);
    
    SELECT 
    competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, descricao_conta, 
    valor_nominal AS receita_realizada
    FROM siops.lancamentos
    WHERE FASE = 'Receitas Realizadas Brutas'
    ORDER BY competencia, ibge, conta;

    --  POST-STATEMENT
    @IF(
      @runtime_stage = 'evaluating',
      COPY siops.receitas TO 'data/outputs/siops__receitas' 
        (FORMAT PARQUET, PARTITION_BY (competencia), OVERWRITE, FILENAME_PATTERN 'siops__receitas_{uuid}')
    );


AUDIT (
  name todos_municipios_tem_receita_IPTU,
  blocking false,
  dialect duckdb
);
-- Para municipios
-- •	(crítica 210) o valor da receita realizada bruta de IPTU (coluna  c), conta 1.1.1.8.01.1.0 não pode ser igual a zero (exceto municípios com código: 130020  130063  130115  130230  290590  292420  251335  140002  140023  140028  140040  140050  140060  140070);
-- somar para dar o valor da mãe
WITH RECURSIVE constants (exceptions) AS (
      SELECT ARRAY[130020, 130063, 130115, 130230, 290590, 292420, 251335, 
                   140002, 140023, 140028, 140040, 140050, 140060, 140070]::int[]
  ),
  receitas_iptu AS (
      SELECT ibge, ente, SUM(receita_realizada) AS receita_iptu_baixa
      FROM siops.receitas 
      -- WHERE fase = 'Receitas Realizadas Brutas' 
      WHERE esfera = 'Municipal' 
      AND conta LIKE '1.1.1.8.01.1%'
      GROUP BY all
  )
  SELECT r.ibge, r.ente, r.receita_iptu_baixa 
  FROM receitas_iptu r, constants c
  WHERE r.receita_iptu_baixa < 5 
  AND NOT (r.ibge = ANY(c.exceptions));


AUDIT (
  name  todos_municipios_tem_receita_ITBI,
  blocking false,
  dialect duckdb
);

WITH RECURSIVE constants (exceptions) AS (
      SELECT ARRAY[130006, 130010, 130020, 130130, 130395, 290020, 290323, 291250,
       291535, 251335, 140040, 140050, 140070, 280200, 280250, 280530]::int[]
  ),
  receitas AS (
      SELECT ibge, ente, SUM(receita_realizada) AS receita_itbi_baixa
      FROM siops.receitas 
      -- WHERE fase = 'Receitas Realizadas Brutas' 
      WHERE esfera = 'Municipal' 
      AND conta LIKE '1.1.1.8.01.4.%'
      GROUP BY all
  )
  SELECT r.ibge, r.ente, r.receita_itbi_baixa 
  FROM receitas r, constants c
  WHERE r.receita_itbi_baixa < 5 
  AND NOT (r.ibge = ANY(c.exceptions));


AUDIT (
  name todos_municipios_tem_receita_CotaParteIPVA,
  blocking false,
  dialect duckdb
);
  
 from siops.receitas where conta = '1.7.2.8.01.2.0' and receita_realizada < 5;

AUDIT (
  name todos_estados_tem_receita_IPVA_acima_minimo,
  blocking false,
  dialect duckdb
);
  
 from siops.receitas where conta = '1.1.1.8.01.2.1' and receita_realizada < @minimo;

AUDIT (
  name todos_estados_tem_receita_ITCMD_acima_minimo,
  blocking false,
  dialect duckdb
);
from siops.receitas where conta = '1.1.1.8.01.2.1' and receita_realizada < @minimo;


AUDIT (
  name todos_municipios_tem_receita_ISS,
  blocking false,
  dialect duckdb
);
SELECT
    *
FROM
    (
        SELECT
            ibge,
            ente,
            SUM(receita_realizada) AS receita_imposto
        FROM
            siops.receitas
        WHERE
            conta LIKE '1.1.1.8.02.3%'
            AND esfera = 'Municipal'
        GROUP BY ALL
    )
WHERE
    receita_imposto < 5;

