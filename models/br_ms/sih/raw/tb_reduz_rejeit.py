import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw_sih.tb_reduz_rejeit",
    columns={
        "DT_CMPT": "TEXT",
        "NU_AIH": "TEXT",
        "CO_IDENT": "TEXT",
        "CO_CNES": "TEXT",
        "NU_ESPECIALIDADE": "TEXT",  
        "CO_PROC_REALIZADO": "TEXT",         
        "NU_NATJUR": "TEXT",
        "QT_DIAS_PERM": "INT",
        "NU_VAL_TOT": "DOUBLE",
        "CO_FINANCIAMENTO": "TEXT",
        "CO_DIAG_PRI": "TEXT",
        "CO_PACIENTE_RACA_COR": "TEXT",
        "CO_ETNIA": "TEXT",
        "NU_IDADE": "TEXT",
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
    password = os.environ['ORACLE_RJPO1DR_PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'RJPO1DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = """
     SELECT  DT_CMPT, NU_AIH, CO_IDENT, CO_CNES, NU_ESPECIALIDADE, CO_PROC_REALIZADO, NU_NATJUR,  QT_DIAS_PERM, NU_VAL_TOT, CO_FINANCIAMENTO,  CO_DIAG_PRI, CO_PACIENTE_RACA_COR, CO_ETNIA, NU_IDADE  FROM AIH.TB_REDUZ WHERE DT_CMPT > '202112' AND DT_CMPT <'202301'
        UNION
     SELECT DT_CMPT, NU_AIH, CO_IDENT, CO_CNES, NU_ESPECIALIDADE, CO_PROC_REALIZADO, NU_NATJUR,  QT_DIAS_PERM, NU_VAL_TOT, CO_FINANCIAMENTO,  CO_DIAG_PRI, CO_PACIENTE_RACA_COR, CO_ETNIA, NU_IDADE  FROM AIH.TB_REJEIT WHERE DT_CMPT > '202112' AND DT_CMPT <'202301'
    """

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise
