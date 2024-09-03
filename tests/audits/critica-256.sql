-- (crítica 256) o valor da receita realizada bruta (coluna e), conta 1.7.1.8.01.6.0 não pode ser inferior/igual a 0,70*base IPI-EXP ou maior que 2,50 base IPI_EXP para Estados e DF
{{ config(
  enabled=false
) }}
SELECT d.VALOR, b.MIN_FROM_IPI_EXP, b.MAX_FROM_IPI_EXP
FROM {{ref('dataset')}} AS d
LEFT JOIN (
    SELECT ano, IPI_EXP * 0.7 AS MIN_FROM_IPI_EXP, IPI_EXP * 2.5 AS MAX_FROM_IPI_EXP, UF
    FROM Base
    WHERE UF IS NOT NULL
) AS b ON d.uf = b.uf
WHERE d.esfera = 'Estadual'
AND d.fase = 'Receitas Realizadas Brutas'
AND d.conta = '1.7.1.8.01.6.0'
AND (d.VALOR < b.MIN_FROM_IPI_EXP or 
 d.VALOR > b.MAX_FROM_IPI_EXP)