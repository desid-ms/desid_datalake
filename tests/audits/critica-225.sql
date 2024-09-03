-- Para municipios
-- •	(crítica 225) o valor da receita realizada bruta (coluna  c), conta 1.1.1.8.02.3.0 não pode ser igual a zero;



select * from (from {{ref('dataset')}} SELECT ENTE, IBGE, UF, fase,  SUM(VALOR) AS total_valor
    WHERE fase = 'Receitas Realizadas Brutas'
    AND esfera = 'Municipal'
    and conta like '1.1.1.8.02.3%'
    GROUP BY ENTE, IBGE, UF, fase)
  where total_valor<5

--  Não está checando se todos os municipios estão apresentando o valor