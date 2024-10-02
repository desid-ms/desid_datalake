import typing as t
from datetime import datetime
import pandas as pd
import cx_Oracle
from sqlmesh import ExecutionContext, model
import os

@model(
    "raw_siopsuf.rl_proj_pasta_item",
    columns={
        "CO_SEQ_PASTA_ITEM": "INT",
        "CO_ANO_PER_ENTE_PASTA": "INT",
        "CO_ITEM": "INT",
        "CO_PASTA_ITEM_ORIGEM": "INT",
        "NU_ORDEM": "INT",
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    username = os.environ['ORACLE_USR']
    password = os.environ['ORACLE_PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'RJPO2DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = "SELECT *  FROM  SIOPSUF.RL_PROJ_PASTA_ITEM"

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise
