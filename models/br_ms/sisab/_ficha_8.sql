select distinct id_atendimento
from raw.sisab__es_atendimentos 
    where id_atendimento in 
        (select id_atendimento from sisab.producao 
            where co_tipo_ficha_atendimento = 8)
    and key = 'MV'
    and value = '19'