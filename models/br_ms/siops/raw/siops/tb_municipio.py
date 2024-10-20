import typing as t
from datetime import datetime
import pandas as pd
import cx_Oracle
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import os

@model(
    "raw.siops__tb_municipio",
    kind = dict (name = ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, unique_key = "CO_MUNICIPIO"),
    columns={
        "CO_MUNICIPIO": "TEXT",
        "CO_UF": "INT",
        "NO_MUNICIPIO": "TEXT",
        "ST_CAPITAL": "TEXT",
        "NU_ANO_INCLUSAO": "INT",
        "NU_ANO_EXCLUSAO": "INT"
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
    query = "SELECT * FROM SIOPS.TB_MUNICIPIO"
    
    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise