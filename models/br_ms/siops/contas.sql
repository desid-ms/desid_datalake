MODEL (
    name siops.contas,
    kind FULL,
    depends_on ["siops.contas_operacionais"]
);
SELECT
          I.CO_SEQ_ITEM AS CODIGO_SIOPS,
          I.CO_ITEM_EXIBICAO AS CONTA,
          I.NO_COMPLETO_ITEM AS DS_CONTA,
          CASE 
              WHEN C.CO_ITEM IS NOT NULL THEN 'Operacionais' 
              ELSE 'Agregadoras' 
          END AS TIPO_CONTA
      FROM
          RAW_SIOPSUF.TB_PROJ_ITEM I
      LEFT JOIN siops.contas_operacionais C ON I.CO_SEQ_ITEM = C.CO_ITEM
      ORDER BY CODIGO_SIOPS