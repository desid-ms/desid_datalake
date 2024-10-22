MODEL (
    name siops.receitas,
    audits (todos_municipios_tem_receita_iptu)
);
    
    SELECT 
    competencia, ibge, ente, capital, regiao, uf, esfera, populacao, conta, ds_conta, 
    valor_nominal AS receita_realizada
    FROM siops.lancamentos
    WHERE FASE = 'Receitas Realizadas Brutas'
    ORDER BY competencia, ibge, conta;


    -- Para municipios
-- •	(crítica 210) o valor da receita realizada bruta de IPTU (coluna  c), conta 1.1.1.8.01.1.0 não pode ser igual a zero (exceto municípios com código: 130020  130063  130115  130230  290590  292420  251335  140002  140023  140028  140040  140050  140060  140070);
-- somar para dar o valor da mãe


AUDIT (
  name todos_municipios_tem_receita_iptu,
  dialect duckdb
);

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