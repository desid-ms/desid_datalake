import typing as t
from datetime import datetime
import pandas as pd
import cx_Oracle
from sqlmesh import ExecutionContext, model
import os

@model(
    "raw_siopsuf.tb_vl_valores",
    columns={
        "CO_PASTA": "INT",
        "CO_ITEM": "TEXT",
        "CO_TIPO": "INT",
        "NU_CNPJ_INSTITUICAO": "TEXT",
        "NU_ANO": "INT",
        "NU_PERIODO": "INT",
        "CO_UF": "INT",
        "CO_INSTITUICAO_CLIENTE": "INT",
        "QT_PERIODOS": "INT",
        "NU_VALOR": "DECIMAL(18,2)",
        "NU_ANO_PREENCHIDO": "INT",
        "NU_PERIODO_PREENCHIDO": "INT",
        "CO_GRUPO": "INT",
        "CO_ITEM_EXIBICAO": "TEXT",
        "CO_PASTA_HIERARQUIA": "TEXT"
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
    query = "SELECT * FROM SIOPSUF.TB_VL_VALORES where NU_ANO > 2017"

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise
