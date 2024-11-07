with todos_procedimentos as (
  select ab.* exclude(codigo_ab) from raw.sha__procedimentos_ab ab 
  union all
  select * from raw.sha__procedimentos
)
  SELECT a.*, p.*
  FROM raw.sisab__es_atendimentos a
  LEFT JOIN todos_procedimentos p
      ON a.value = p.codigo_procedimento
  WHERE a.id_atendimento IN (
      SELECT id_atendimento
      FROM raw.sisab__es
      WHERE co_tipo_ficha_atendimento in (7, 5)
      AND key = 'PRC'
  )