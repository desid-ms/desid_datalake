MODEL (
    name raw.sih__especialidades,
    kind FULL
);
-- fonte: arquivo ESPECIAL.CNV / TAB_SIH_200308-200712
SELECT * FROM read_csv_auto('data/inputs/sha/sih_especialidade.csv');
