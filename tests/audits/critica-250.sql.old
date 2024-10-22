-- para municipios
-- •	(crítica 250) o valor da receita realizada bruta (coluna  c), conta 1.7.2.8.01.2.0 não pode ser igual a zero;

select distinct IBGE, ENTE, VALOR from {{ ref('dataset') }} where esfera='Municipal' and IBGE not in (select distinct IBGE from {{ref('dataset')}} where fase = 'Receitas Realizadas Brutas' and conta = '1.7.2.8.01.2.0' and VALOR>0)