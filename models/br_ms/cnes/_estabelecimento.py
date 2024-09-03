import typing as t
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName


@model(
    "cnes.raw_estabelecimento",
    # kind=dict(
    #     name=ModelKindName.INCREMENTAL_BY_TIME_RANGE,
    #     time_column="COMPETENCIA"
    # ),
    kind=dict(name=ModelKindName.FULL),

    columns={
        "COMPETENCIA": "DATE",
        "CNES": "STRING",
        "NAT_JUR": "STRING",
        "TP_PREST": "STRING",
        "TP_UNID": "STRING",
        "TPGESTAO": "STRING",
        "CODUFMUN": "STRING",
        "CPF_CNPJ": "STRING",
        "CNPJ_MAN": "STRING"
        }
        
)


def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    client = bigquery.Client()

    query = """
    SELECT
        DATE(CAST(ANO AS INT64), CAST(MES AS INT64), 1) AS COMPETENCIA,
        CNES,
        NAT_JUR,
        TP_PREST,
        TP_UNID,
        TPGESTAO,
        CODUFMUN,
        CPF_CNPJ,
        CNPJ_MAN
        FROM `basedosdados-staging.br_ms_cnes_staging.estabelecimento`
        LIMIT 10;

        """
        
    dt = {
            'COMPETENCIA': 'datetime64[ns]',
            'CNES': 'string',
            'NAT_JUR': 'string',
            'TP_PREST': 'string',
            'TP_UNID': 'string',
            'TPGESTAO': 'string',
            'CODUFMUN': 'string',
            'CPF_CNPJ': 'string',
            'CNPJ_MAN': 'string'
        }

    return client.query(query).to_dataframe().astype(dt)
