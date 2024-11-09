import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import oracledb
import os
from lib.db_utils import OracleConfig, QueryExecutor


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
):  # Added return type annotation
    
    config = OracleConfig('DFPO1DR.saude.gov')
    executor = QueryExecutor(config)
       
    query = """
        SELECT a.*
        FROM (
            SELECT 
                b.*,
                ROW_NUMBER() OVER (ORDER BY ID_ATENDIMENTO) as rn
            FROM DBSISAB.VW_SISAB_CGES b
            WHERE COMPETENCIA BETWEEN :start_date AND :end_date
            AND ST_REGISTRO_ATIVO = 'S'
        ) a
        WHERE a.rn > :offset
        ORDER BY a.rn
    """
    
    params = {
        'start_date': '202201',
        'end_date': '202212',
        'offset': executor.checkpoint.current_offset
    }
    
    for chunk in executor.execute_query(query, params):
        yield chunk