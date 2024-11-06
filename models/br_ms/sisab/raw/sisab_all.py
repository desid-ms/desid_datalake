import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
   "raw.sisab",
   columns={
       "ANO": "INTEGER",
       "COMPETENCIA": "INTEGER",
       "CO_TIPO_FICHA_ATENDIMENTO": "BIGINT",
       "ID_ATENDIMENTO": "BIGINT",
       "ID_INDIVIDUO_CNS": "BIGINT", 
       "CO_UF_IBGE": "TEXT",
       "CO_MUNICIPIO_IBGE": "TEXT",
       "CO_CNES": "TEXT",
       "TP_UNIDADE": "TEXT",
       "CO_EQUIPE": "TEXT",
       "TP_EQUIPE": "TEXT",
       "NU_IDADE_ANO": "INTEGER",
       "NU_IDADE_MES": "INTEGER",
       "CO_SEXO": "INTEGER",
       "CO_LOCAL_ATENDIMENTO": "BIGINT",
       "DS_ATENDIMENTO": "TEXT",
       "DS_ATENDIMENTO_JSON": "TEXT",
       "CO_TIPO_ATENDIMENTO": "BIGINT",
       "CO_CBO_OCUPACAO": "TEXT",
       "CO_ATENDIMENTO_ESUSAB": "BIGINT",
       "NO_ARQUIVO_RNDS": "TEXT",
       "ST_ENVIO_RNDS": "TEXT",
       "DS_VALIDACAO_RNDS": "TEXT",
       "CO_PROF_ESUSAB": "BIGINT",
       "CO_AD_MODALIDADE": "BIGINT",
       "CO_AD_ORIGEM": "BIGINT",
       "CO_AD_TIPO_ELEGIVEL": "BIGINT",
       "CO_CONDUTA_DESFECHO": "BIGINT",
       "ST_REGISTRO_ATIVO": "TEXT",
       "CO_CDS_TIPO_ORIGEM": "BIGINT"
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
    password = os.environ['ORACLE_PWD']
    host = 'exaccrjdr-scan.saude.gov'
    port = '1521'
    service_name = 'DFPO1DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = """
        SELECT *
        FROM DBSISAB.VW_SISAB_CGES
        WHERE COMPETENCIA BETWEEN '202201' AND '202212'
        -- AND CO_UF_IBGE = '32'  -- Espirito Santo
        AND ST_REGISTRO_ATIVO = 'S'
        -- AND CO_TIPO_FICHA_ATENDIMENTO IN ('4', '5', '7', '8')
    """

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise


    