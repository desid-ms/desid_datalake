import typing as t
from datetime import datetime
import pandas as pd
import cx_Oracle
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import os

@model(
    "raw_siops.th_homologacao",
    columns={
        "ENTE": "INT",
        "NU_ANO": "INT",
        "NU_PERIODO": "INT",
        "NU_SEQ": "INT",
        "DT_HOMOLOGACAO": "TEXT",
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
    query = """
        SELECT
            CO_MUNICIPIO AS ENTE,
            NU_ANO,
            NU_PERIODO,
            MAX(NU_SEQ) AS NU_SEQ,
            MAX(DT_HOMOLOGACAO) AS DT_HOMOLOGACAO 
        FROM
            SIOPS.TH_AUT_TRANSM
        WHERE
            NU_ANO > 2017
            AND ST_HOMOLOGADO = 'S'
        GROUP BY
            CO_MUNICIPIO, NU_ANO, NU_PERIODO

        UNION ALL

        SELECT
            CO_UF AS ENTE,
            NU_ANO,
            NU_PERIODO,
            MAX(NU_SEQ) AS NU_SEQ,
            MAX(DT_HOMOLOGACAO) AS DT_HOMOLOGACAO 
        FROM
            SIOPSUF.TH_AUT_TRANSM
        WHERE
            NU_ANO > 2017
            AND ST_HOMOLOGADO = 'S'
        GROUP BY
            CO_UF, NU_ANO, NU_PERIODO
    """
    
    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise