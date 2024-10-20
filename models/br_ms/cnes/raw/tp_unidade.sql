MODEL (
    name raw.cnes__tp_unidade,
    kind FULL
);

SELECT * FROM read_csv_auto('data/inputs/sha/tp_unidade.csv', 
    delim='!', 
    header=true, 
    columns={
        'CODIGO': 'VARCHAR',
        'TIPO': 'VARCHAR',
        'CONCEITO': 'VARCHAR'
    },
    ignore_errors=true
);
