MODEL (
    name siops.contas_operacionais,
    kind VIEW
);
    
select 
    co_item::text as co_item from 
    (
        select co_item::int as co_item from raw_siopsuf.tb_vl_valores
        union
        select co_item::int as co_item from raw_siops.tb_vl_valores
        order by co_item
    )