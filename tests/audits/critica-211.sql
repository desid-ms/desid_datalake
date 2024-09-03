select *
from {{ ref('dataset') }}
where fase = 'Receitas Realizadas Brutas'
    and conta = '1.1.1.8.01.2.1'
    and valor < 5_000_000