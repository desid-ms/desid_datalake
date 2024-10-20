import typing as t
from datetime import datetime
import pandas as pd
import cx_Oracle
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import os

@model(
    "raw.siops__th_aut_transm",
    columns={
        "CO_MUNICIPIO": "INT",
        "NU_ANO": "INT",
        "NU_PERIODO": "INT",
        "NU_SEQ": "INT",
        "DT_HOMOLOGACAO": "TEXT",
        "ST_HOMOLOGADO": "TEXT",
    
    },
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) :
    
    username = os.environ['ORACLE_USR']
    password = os.environ['ORACLE_PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'RJPO2DR.saude.gov'  
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = """
        SELECT
            CO_MUNICIPIO,
            NU_ANO,
            NU_PERIODO,
            NU_SEQ,
            DT_HOMOLOGACAO, 
            ST_HOMOLOGADO 
        FROM
            SIOPS.TH_AUT_TRANSM
    """
    
    try:
        # # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            for chunk in pd.read_sql(query, connection, chunksize = 1_000_000):
                yield chunk

    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise