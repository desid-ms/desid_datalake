import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw_sisab.vw_sisab_cges",
    columns={
          "ANO": "INT",
          
"COMPETENCIA": "INT",

"CO_TIPO_FICHA_ATENDIMENTO": "INT",
"CO_CNES":"TEXT",
"CO_LOCAL_ATENDIMENTO": "INT",
"DS_ATENDIMENTO_JSON": "TEXT",
"CO_TIPO_ATENDIMENTO": "INT",
"CO_CBO_OCUPACAO": "TEXT",
}
)

def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    username = os.environ['ORACLE_USR']
    password = os.environ['ORACLE__PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'DFPO1DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = "SELECT ANO, COMPETENCIA, CO_TIPO_FICHA_ATENDIMENTO, CO_CNES, CO_LOCAL_ATENDIMENTO, DS_ATENDIMENTO_JSON, CO_TIPO_ATENDIMENTO, CO_CBO_OCUPACAO FROM DBSISAB.VW_SISAB_CGES where ANO = 2022"

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise


    