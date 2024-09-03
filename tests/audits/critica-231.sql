-- (crítica 231) o valor da receita realizada bruta (coluna e), conta 1.7.1.8.01.1.0 não pode ser inferior a 0,85*base FPE ou maior que 1,50 base FPE para estados e DF
{{ config(
  enabled=false
) }}
SELECT d.*
FROM {{ref('dataset')}} AS d
LEFT JOIN (
    SELECT ano, "FPE 100%" * 0.85 AS MIN_FROM_FPE, "FPE 100%" * 1.5 AS MAX_FROM_FPE, UF
    FROM Base
    WHERE UF IS NOT NULL
) AS b ON d.uf = b.uf
WHERE d.esfera = 'Estadual'
AND d.fase = 'Receitas Realizadas Brutas'
AND d.conta = '1.7.1.8.01.1.0'
AND (d.VALOR < b.MIN_FROM_FPE
or d.VALOR > b.MAX_FROM_FPE)