-- •	(críticas 271, 276, 282, 284, 286, 288) O valor_NOMINAL da dedução para formação do FUNDEB coluna (t) das contas 1.7.1.8.01.1.0, 1.1.1.8.02.0.0, 1.7.1.8.01.6.0, 1.1.1.8.01.2.0, 1.1.1.8.01.3.0 e 1.7.1.8.06.0.0 e  não pode ser inferior a 0,18*valor_NOMINAL da receita realizada bruta ou superior a 0,22 da receita realizada bruta da conta em questão;


WITH FILTRO_FUNDEB AS (
    select ENTE,
        VALOR_NOMINAL as TRANSF_FUNDEB,
        0.18 * VALOR_NOMINAL AS MIN_FUNDEB,
        0.22 * VALOR_NOMINAL AS MAX_FUNDEB
    from siops.lancamentos
    where esfera = 'Estadual'
        and fase = 'Receitas Realizadas Brutas'
        and conta = '1.7.1.8.05.0.0'
),
ESTADOS AS (
    select *
    from siops.lancamentos
    where esfera = 'Estadual'
        and fase = 'Receitas Realizadas Brutas'
        and conta in (
            '1.7.1.8.01.1.0',
            '1.1.1.8.02.0.0',
            '1.7.1.8.01.6.0',
            '1.1.1.8.01.2.0',
            '1.1.1.8.01.3.0',
            '1.7.1.8.06.0.0'
        )
)
select 
    E.ENTE,
    E.CONTA,
    E.VALOR_NOMINAL,
    F.TRANSF_FUNDEB,
    F.MIN_FUNDEB,
    F.MAX_FUNDEB
from ESTADOS as E
    join FILTRO_FUNDEB as f on E.ENTE = f.ENTE
where VALOR_NOMINAL < MIN_FUNDEB
    or VALOR_NOMINAL > MAX_FUNDEB