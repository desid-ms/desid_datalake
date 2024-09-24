import typing as t
from datetime import datetime
import pandas as pd
import cx_Oracle
from sqlmesh import ExecutionContext, model
import os

@model(
    "raw_siopsuf.tb_proj_pasta",
    columns={
        "CO_SEQ_PASTA": "INT",
        "TP_PASTA": "TEXT",
        "NO_PASTA": "TEXT",
        "CO_PASTA_INTERNO": "TEXT",
        "NO_COMPLETO_PASTA": "TEXT",
        "DS_FUNCIONALIDADE_PASTA": "TEXT",
        "DT_INCLUSAO": "DATE"
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
    query = "SELECT * FROM SIOPSUF.TB_PROJ_PASTA"

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise
