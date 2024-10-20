MODEL (
  name raw.sha__funcao_procedimento,
  kind FULL
);

SELECT * FROM read_csv_auto('data/inputs/sha/procedimento_sha2.csv') order by all