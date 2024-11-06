WITH todos_procedimentos as (
      SELECT
          a.co_procedimento_ab as co_procedimento,
          p.nome_proc as procedimento,
          p.fonte,
          p.co_financiamento,
          p.valor_2022
      FROM ab_sigtap a
      LEFT JOIN procedimentos p
          ON a.co_procedimento_sigtap = p.proc_rea
      UNION ALL
      SELECT
          proc_rea as co_procedimento,
          nome_proc as procedimento,
          fonte,
          co_financiamento,
          valor_2022
      FROM procedimentos p
  )
  SELECT a.*, p.co_procedimento, p.procedimento, p.fonte, p.co_financiamento, p.valor_2022
  FROM raw.sisab__es_atendimentos a
  LEFT JOIN todos_procedimentos p
      ON a.value = p.co_procedimento
  WHERE a.id_atendimento IN (
      SELECT id_atendimento
      FROM raw.sisab__es
      WHERE co_tipo_ficha_atendimento in (7, 5)
      AND key = 'PRC'
  )