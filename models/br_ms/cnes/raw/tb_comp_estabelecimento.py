import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw.cnes__tb_comp_estabelecimento",
    # kind = dict (name = ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, unique_key = "CO_UNIDADE"),
    columns={
        
        "NU_COMP": "TEXT",
        "CO_CNES": "TEXT",
        "NO_FANTASIA": "TEXT",
        "DT_ATUALIZACAO": "TEXT",  
        "TP_UNIDADE": "TEXT",
        'TP_PFPJ'   : 'TEXT',
        "NU_CNPJ_MANTENEDORA": "TEXT",
        "TP_GESTAO": "TEXT",
        "CO_MUNICIPIO_GESTOR":"TEXT",
        "TP_PRESTADOR": "TEXT",
        "NU_LATITUDE": "TEXT",
        "NU_LONGITUDE": "TEXT",
        "CO_NATUREZA_ORGANIZACAO": "TEXT",
        "CO_NATUREZA_JUR": "TEXT"}
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
        SELECT NU_COMP, CO_CNES, NO_FANTASIA, TP_PFPJ, NU_CNPJ_MANTENEDORA, TP_GESTAO, 
        CO_NATUREZA_JUR, CO_NATUREZA_ORGANIZACAO, TP_UNIDADE,  TP_GESTAO, CO_MUNICIPIO_GESTOR, TP_PRESTADOR, 
        NU_LATITUDE, NU_LONGITUDE, CO_NATUREZA_JUR, DT_ATUALIZACAO 
        FROM CNES.TB_COMP_ESTABELECIMENTO tce 
        WHERE NU_COMP >='202201' AND NU_COMP <='202212'
    """

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise