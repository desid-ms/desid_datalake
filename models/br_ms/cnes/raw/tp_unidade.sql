MODEL (
    name raw_cnes.tp_unidade,
    kind FULL
);

SELECT * FROM read_csv_auto('data/inputs/tp_unidade.csv', 
    delim='!', 
    header=true, 
    columns={
        'CODIGO': 'VARCHAR',
        'TIPO': 'VARCHAR',
        'CONCEITO': 'VARCHAR'
    },
    ignore_errors=true
);
