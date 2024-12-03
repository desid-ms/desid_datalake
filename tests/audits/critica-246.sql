-- •	(crítica 246) o valor da receita realizada bruta (coluna e), conta 1.1.1.8.02.0.0 não pode ser inferior a 0,80*base ICMS ou maior que 1,50 base ICMS. para Estados e DF

SELECT distinct d.conta
FROM siops.lancamentos AS d
LEFT JOIN (
    SELECT *
    FROM Base
    WHERE UF IS NOT NULL
) AS b ON d.uf = b.uf
WHERE d.esfera = 'Estadual'
AND d.fase = 'Receitas Realizadas Brutas'
AND d.conta = '1.1.1.8.02.0.0'
order by d.conta
