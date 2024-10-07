MODEL (
    name siops.periodos,
    kind FULL
);

  SELECT
      y || '-' || b AS PERIODO,
      y AS ANO,
      b AS BIMESTRE,
      MAKE_DATE(y, (b - 1) * 2 + 1, 1)::timestamp AS DATA_INICIO,
      MAKE_DATE(y, (b - 1) * 2 + 1, 1) + INTERVAL '2 MONTHS' - INTERVAL '1 DAY' AS DATA_FIM
  FROM
      generate_series(2018, 2023) AS y(y)
  CROSS JOIN
      generate_series(1, 6) AS b(b)
  ORDER BY
      y, b


