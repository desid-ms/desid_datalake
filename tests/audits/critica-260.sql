-- •	(crítica 260) o valor da receita realizada bruta (coluna e), conta 1.7.1.8.05.0.0 não pode ser inferior a 0,8*base FUNDEB ou superior a 1,20*base FUNDEB. para Estados e DF
{{ config(
  enabled=false
) }}
SELECT d.VALOR, b.MIN_FROM_FUNDEB, b.MAX_FROM_FUNDEB
  FROM {{ref('dataset')}} AS d
  LEFT JOIN (
      SELECT ano, FUNDEB * 0.8 AS MIN_FROM_FUNDEB, FUNDEB * 2.5 AS MAX_FROM_FUNDEB, UF
      FROM Base
      WHERE UF IS NOT NULL
  ) AS b ON d.uf = b.uf
  WHERE d.esfera = 'Estadual'
  AND d.fase = 'Receitas Realizadas Brutas'
  AND d.conta = '1.7.1.8.05.0.0'
  AND d.VALOR > b.MAX_FROM_FUNDEB
  and d.valor < b.MIN_FROM_FUNDEB 