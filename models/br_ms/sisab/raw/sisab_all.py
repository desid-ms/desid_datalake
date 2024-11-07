import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import oracledb
import os


oracledb.init_oracle_client()


def get_pool():
    host = 'exaccrjdr-scan.saude.gov'
    port = '1521'
    service_name = 'DFPO1DR.saude.gov'
    dsn = f"""(DESCRIPTION=
            (ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port}))
            (CONNECT_DATA=(SERVICE_NAME={service_name})))"""
            
    return oracledb.create_pool(
        user=os.environ['ORACLE_USR'],
        password=os.environ['ORACLE_PWD'],
        dsn=dsn, 
        min=1,
        max=5,
        increment=1,
        encoding="UTF-8",
        threaded=True  # Enable threading support
    )

@model(
    "raw.sisab",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key="ID_ATENDIMENTO"
    ),
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
) -> pd.DataFrame:  # Added return type annotation
    
    query = """
        SELECT *
        FROM DBSISAB.VW_SISAB_CGES
        WHERE COMPETENCIA BETWEEN :start_date AND :end_date
        AND ST_REGISTRO_ATIVO = 'S'
    """
    
    params = {
        'start_date': '202201',
        'end_date': '202212'
    }
    
    pool = None
    try:
        pool = get_pool()
        with pool.acquire() as connection:
            for chunk in pd.read_sql(
                query, 
                con=connection, 
                params=params,
                chunksize=4_000_000
            ): yield chunk            
    
    except oracledb.Error as error:
        print(f"Oracle Database Error: {error}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        if pool:
            pool.close()