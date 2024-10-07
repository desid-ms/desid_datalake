MODEL (
    name siops.contas_periodo,
    kind FULL
);
SELECT 
      conta,
      last(nome_conta) AS nome_conta,
      last(tipo_conta) AS tipo_conta,
      last(data_atualizacao) AS data_atualizacao,
      periodo
  FROM 
      siops__dev.historico_contas
  GROUP BY 
      conta, periodo
