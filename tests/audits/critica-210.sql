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
      SELECT codigo_ibge, SUM(valor_receita) AS total_receita_realizada
      FROM siops.receitas 
      WHERE fase = 'Receitas Realizadas Brutas' 
      AND esfera = 'Municipal' 
      AND codigo_conta LIKE '1.1.1.8.01.1%'
      GROUP BY codigo_ibge
  )
  SELECT r.codigo_ibge, r.total_receita_realizada 
  FROM receitas_iptu r, constants c
  WHERE r.total_receita_realizada < 5 
  AND NOT (r.codigo_ibge = ANY(c.exceptions));