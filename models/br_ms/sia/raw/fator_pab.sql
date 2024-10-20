MODEL (
    name raw.sia__valor_pab,
    kind FULL
);

SELECT * FROM st_read('data/inputs/sha/valor_PAB_2020_2022.xlsx');
