import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import oracledb
import os

@model(
    "raw.cnes__tb_comp_estabelecimento",
    # kind = dict (name = ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, unique_key = "CO_UNIDADE"),
    columns={
        "NU_COMP": "TEXT",
        "CO_CNES": "TEXT",
        "NO_FANTASIA": "TEXT",
        "TP_PFPJ": "TEXT",
        "NU_CNPJ_MANTENEDORA": "TEXT",
        "CO_NATUREZA_JUR": "TEXT",
        "CO_NATUREZA_ORGANIZACAO": "TEXT",
        "TP_UNIDADE": "TEXT",
        "TP_PRESTADOR": "TEXT",
        "TP_GESTAO": "TEXT",
        "CO_ESFERA_ADMINISTRATIVA": "TEXT",  
        "CO_ESTADO_GESTOR": "TEXT",          
        "CO_MUNICIPIO_GESTOR": "TEXT",
        "CO_REGIAO_SAUDE": "TEXT",           
        "CO_MICRO_REGIAO": "TEXT",           
        "CO_DISTRITO_SANITARIO": "TEXT",      
        "NU_LATITUDE": "TEXT",
        "NU_LONGITUDE": "TEXT",
        "DT_ATUALIZACAO": "TEXT"
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
    dsn = f"{host}:{port}/{service_name}"
    query = """
SELECT
    NU_COMP,
    CO_CNES,
    NO_FANTASIA,
    TP_PFPJ,
    NU_CNPJ_MANTENEDORA,
    CO_NATUREZA_JUR,
    CO_NATUREZA_ORGANIZACAO,
    TP_UNIDADE,
    TP_PRESTADOR,
    TP_GESTAO,
    CO_ESFERA_ADMINISTRATIVA,
    CO_ESTADO_GESTOR,
    CO_MUNICIPIO_GESTOR,
    CO_REGIAO_SAUDE,
    CO_MICRO_REGIAO,
    CO_DISTRITO_SANITARIO,
    NU_LATITUDE,
    NU_LONGITUDE,
    DT_ATUALIZACAO
FROM
    CNES.TB_COMP_ESTABELECIMENTO tce
        WHERE NU_COMP >='202201' AND NU_COMP <='202212'
    """

    try:
        # Establish the connection
        with oracledb.connect(user=username, password=password, dsn=dsn) as connection:
            for chunk in pd.read_sql(query, connection, chunksize = 6_000_000):
                yield chunk
    except oracledb.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise
    
    