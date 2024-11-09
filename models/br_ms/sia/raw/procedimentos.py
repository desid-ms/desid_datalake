import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw.sia__procedimentos",
    kind = dict (name = ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column ="DT_CMP"),
    start = "2015-01-01",
    end = "2022-12-31",
    # SELECT DT_CMP , CO_PA_ID, NU_PA_TOTAL, DS_PA_DC  FROM SIA.TB_PROCEDIMENTO_AMBULATORIAL tpa 
    columns={
        "DT_CMP": "TEXT",
        "CO_PA_ID": "TEXT",
        "CO_PA_DV": "TEXT",
        "NU_PA_TOTAL": "DOUBLE",
        "DS_PA_DC": "TEXT",
    },
    column_descriptions={
      
        "DT_CMP": "COMPETÊNCIA",
        "CO_PA_ID": "IDENTIFICADOR DO PROCEDIMENTO",
        "CO_PA_DV": "DIGITO VERIFICADOR DO PROCEDIMENTO",
        "NU_PA_TOTAL": "VALOR_PROCEDIMENTO",
        "DS_PA_DC": "DESCRICAO PROCEDIMENTO",
    
    },
)

def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
):

    username = os.environ['ORACLE_USR']
    password = os.environ['ORACLE_RJPO1DR_PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'RJPO1DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = """
        SELECT DT_CMP , CO_PA_ID, CO_PA_DV, NU_PA_TOTAL, DS_PA_DC  FROM SIA.TB_PROCEDIMENTO_AMBULATORIAL
    """
    try:
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            for chunk in pd.read_sql(query, connection, chunksize = 4_000_000):
                yield chunk

    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise
