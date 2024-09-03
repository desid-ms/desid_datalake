
select *
from {{ ref('dataset') }}
where fase = 'Receitas Realizadas Brutas'
    and conta = '1.1.1.8.01.3.1'
    and VALOR < 460_000
