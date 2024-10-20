MODEL (
    name staging.siops__periodos,
    kind FULL
);

  SELECT
      y || '-' || b AS PERIODO,
      y AS ANO,
      b AS BIMESTRE,
      CASE
        WHEN b = 1 THEN '12'
        WHEN b = 2 THEN '14'
        WHEN b = 3 THEN '1'
        WHEN b = 4 THEN '18'
        WHEN b = 5 THEN '20'
        WHEN b = 6 THEN '2'
      END AS "CODIGO_BIMESTRE_SIOPS",
      MAKE_DATE(y, (b - 1) * 2 + 1, 1)::timestamp AS DATA_INICIO,
      MAKE_DATE(y, (b - 1) * 2 + 1, 1) + INTERVAL '2 MONTHS' - INTERVAL '1 DAY' AS DATA_FIM
  FROM
      generate_series(2018, 2023) AS y(y)
  CROSS JOIN
      generate_series(1, 6) AS b(b)
  ORDER BY
      y, b

