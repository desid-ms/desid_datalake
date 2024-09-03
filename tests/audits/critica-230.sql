-- Para municipios
-- •	•	(crítica 230) o valor da receita realizada bruta (coluna c), conta 1.7.1.8.01.2.0 não pode ser inferior a 0,8*base FPM ou maior que 1,50 base FPM;

SELECT r.ano,
  r.ibge,
  r.valor,
  b.MIN,
  b.MAX
FROM (
    SELECT *
    FROM {{ref('receitas')}}
    WHERE conta = '1.7.1.8.01.2.0'
      AND fase = 'Receitas Realizadas Brutas'
      AND ano = 2022
  ) AS r
  JOIN (
    SELECT ibge,
      CAST(0.85 * FPM AS DECIMAL(14, 2)) AS MIN,
      CAST(1.5 * FPM AS DECIMAL(14, 2)) AS MAX
    FROM {{ref('base_municipal')}}
  ) AS b ON r.ibge = b.ibge
WHERE (
    r.valor < b.MIN
    OR r.valor > b.MAX
  )
-- FPM??
-- SELECT d.VALOR, b.MIN_FROM_FUNDEB, b.MAX_FROM_FUNDEB
-- FROM dataset AS d
-- LEFT JOIN (
--     SELECT FPM, FUNDEB * 0.8 AS MIN_FROM_FUNDEB, FUNDEB * 2.5 AS MAX_FROM_FUNDEB, UF
--     FROM Base
--     WHERE UF IS NOT NULL
-- ) AS b ON
-- select VALOR from dataset as d 
-- join  
--     where 
--           fase = 'Receitas Realizadas Brutas'
--           and esfera='Municipal'
--           and CODIGO ='1.7.1.8.01.2.0'
--           and esfera='Municipal';