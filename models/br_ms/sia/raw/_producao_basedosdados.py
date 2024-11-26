import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from tqdm import tqdm
import duckdb
import os

@model(
    "raw.sia__producao_basedosdados",
    kind=dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="DT_CMP"),
    start="2015-01-01",
    end="2022-12-31",
    
    columns={
        "PA_CODUNI": "TEXT",
        "PA_GESTAO": "TEXT",
        "PA_CONDIC": "TEXT",
        "PA_UFMUN": "TEXT",
        "PA_REGCT": "TEXT",
        "PA_INCOUT": "TEXT",
        "PA_INCURG": "TEXT",
        "PA_TPUPS": "TEXT",
        "PA_TIPPRE": "TEXT",
        "PA_MN_IND": "TEXT",
        "PA_CNPJCPF": "TEXT",
        "PA_CNPJMNT": "TEXT",
        "PA_CNPJ_CC": "TEXT",
        "PA_MVM": "TEXT",
        "PA_CMP": "TEXT",
        "PA_PROC_ID": "TEXT",
        "PA_TPFIN": "TEXT",
        "PA_SUBFIN": "TEXT",
        "PA_NIVCPL": "TEXT",
        "PA_DOCORIG": "TEXT",
        "PA_AUTORIZ": "TEXT",
        "PA_CNSMED": "TEXT",
        "PA_CBOCOD": "TEXT",
        "PA_MOTSAI": "TEXT",
        "PA_OBITO": "TEXT",
        "PA_ENCERR": "TEXT",
        "PA_PERMAN": "TEXT",
        "PA_ALTA": "TEXT",
        "PA_TRANSF": "TEXT",
        "PA_CIDPRI": "TEXT",
        "PA_CIDSEC": "TEXT",
        "PA_CIDCAS": "TEXT",
        "PA_CATEND": "TEXT",
        "PA_IDADE": "TEXT",
        "IDADEMIN": "TEXT",
        "IDADEMAX": "TEXT",
        "PA_FLIDADE": "TEXT",
        "PA_SEXO": "TEXT",
        "PA_RACACOR": "TEXT",
        "PA_MUNPCN": "TEXT",
        "PA_QTDPRO": "TEXT",
        "PA_QTDAPR": "TEXT",
        "PA_VALPRO": "TEXT",
        "PA_VALAPR": "TEXT",
        "PA_UFDIF": "TEXT",
        "PA_MNDIF": "TEXT",
        "PA_DIF_VAL": "TEXT",
        "NU_VPA_TOT": "TEXT",
        "NU_PA_TOT": "TEXT",
        "PA_INDICA": "TEXT",
        "PA_CODOCO": "TEXT",
        "PA_FLQT": "TEXT",
        "PA_FLER": "TEXT",
        "PA_ETNIA": "TEXT",
        "PA_VL_CF": "TEXT",
        "PA_VL_CL": "TEXT",
        "PA_VL_INC": "TEXT",
        "PA_SRV_C": "TEXT",
        "PA_INE": "TEXT",
        "PA_NAT_JUR": "TEXT",
        "ano": "INTEGER",
        "mes": "INTEGER",
        "sigla_uf": "TEXT",
        "DT_CMP": "TEXT"
    }
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
):
    try:
        ano = 2022
        
        for mes in tqdm(range(1, 13)):
            query = f"""
            SELECT * 
            FROM read_parquet('data/inputs/basedosdados/sia_data/ano={ano}/mes={mes}/**/*.parquet')
            """
            
            try:
                df = duckdb.sql(query).df()
                if not df.empty:
                    yield df  # Yield each month's dataframe
                del df
                    
            except Exception as e:
                context.log.error(f"Error processing {ano}-{mes}: {str(e)}")
                raise
                
    except Exception as e:
        print(f"Error processing _producao_ambulatorial_: {str(e)}")
        raise