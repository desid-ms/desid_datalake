-- Para municipios
-- o valor da receita realizada bruta (coluna  c), conta 1.1.1.8.01.4.0 não pode ser igual a zero (exceto municípios com código: 130006  130010  130020  130130  130395  290020  290323  291250  291535  251335  140040  140050  140070  280200  280250  280530);
-- soma


select * from (from {{ref('dataset')}} SELECT ENTE, IBGE, UF, fase,  SUM(VALOR) AS total_valor
    WHERE fase = 'Receitas Realizadas Brutas'
    AND esfera = 'Municipal'
    AND conta LIKE '1.1.1.8.01.4.%'
    GROUP BY ENTE, IBGE, UF, fase)
  -- critica tem excessoes
  where IBGE not in (130006, 130010, 130020, 130130, 130395, 290020, 290323, 291250, 291535, 251335, 140040, 140050, 140070, 280200, 280250, 280530)
  and total_valor<5
  
