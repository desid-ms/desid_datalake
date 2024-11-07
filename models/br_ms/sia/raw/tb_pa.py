import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw.sia__tb_pa",
    kind = dict (name = ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column ="CO_PA_CMP"),
    start = "2022-01-01",
    end = "2022-12-31",
    columns={
      
        "CO_PA_CMP": "TEXT",
        "CO_PA_CODUNI": "TEXT",
        "CO_PA_GESTAO": "TEXT",
        "CO_PA_TPUPS": "TEXT",
        "CO_NATUREZA_JUR": "TEXT",
        "CO_PA_TPFIN": "TEXT",
        "CO_PA_TIPPRE": "TEXT",
        "CO_PA_CBOCOD": "TEXT",
        "CO_PA_PROC_ID": "TEXT",
        "LINHAS": "INT",
        "SUM_NU_PA_QTDPRO": "INT",
        "SUM_NU_PA_QTDAPR": "INT",
        "SUM_NU_PA_VALPRO": "DOUBLE",
        "SUM_NU_PA_VALAPR": "DOUBLE",
        "SUM_NU_VPA_TOT": "DOUBLE",
        "SUM_NU_PA_TOT": "DOUBLE",
      
    },
    column_descriptions={
      
        "CO_PA_CMP": "COMPETÊNCIA",
        "CO_PA_CODUNI": "CÓDIGO CNES",
        "CO_PA_GESTAO": "CÓDIGO IBGE",
        "CO_PA_TPUPS": "TIPO UNIDADE",
        "CO_NATUREZA_JUR": "CÓDIGO DA NATUREZA JURIDICA",
        "CO_PA_TPFIN": "TIPO FINANCIAMENTO",
        "CO_PA_TIPPRE": "TIPO DE PRESTADOR",
        "CO_PA_CBOCOD": "CBO DO PROFISSIONAL DE SAÚDE RESPONSÁVEL", # Código da Ocupação do profissional responsável na  Classificação Brasileira de Ocupações8 (Ministério do Trabalho)
        "CO_PA_PROC_ID": "CÓDIGO DO PROCEDIMENTO NO SISTEMA SIGTAP", # CÖDIGO DO PROCEDIMENTO AMBULATORIAL
        "LINHAS": "QUANTIDADE DE LINHAS AGREGADAS NA SOMA",
        "SUM_NU_PA_QTDPRO": "QUANTIDADE PRODUZIDA",
        "SUM_NU_PA_QTDAPR": "QUANTIDADE APROVADA",
        "SUM_NU_PA_VALPRO": "VALOR PRODUZIDO",
        "SUM_NU_PA_VALAPR": "VALOR APROVADO",
        "SUM_NU_VPA_TOT": "SOMA DOS VALORES UNITÁRIOS NA TABELA VPA",
        # Valor Unitário do Procedimento da Tabela VPA
        "SUM_NU_PA_TOT": "SOMA DOS VALORES UNITÁRIOS NA TABELA SIGTAP", 
        # Valor Unitário do Procedimento da Tabela SIGTAP
        
    },
)

def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
):

    username = os.environ['ORACLE_USR']
    password = os.environ['ORACLE_RJPO1DR_PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'RJPO1DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = """
        SELECT
            CO_PA_CMP, CO_PA_CODUNI, CO_PA_GESTAO, CO_PA_TPUPS, CO_NATUREZA_JUR,
            CO_PA_TPFIN, CO_PA_TIPPRE, CO_PA_CBOCOD, CO_PA_PROC_ID,
            COUNT(*) as LINHAS,
            SUM(NU_PA_QTDPRO) AS SUM_NU_PA_QTDPRO,
            SUM(NU_PA_QTDAPR) AS SUM_NU_PA_QTDAPR,
            CAST(SUM(NU_PA_VALPRO) AS DECIMAL(18,2)) AS SUM_NU_PA_VALPRO,
            CAST(SUM(NU_PA_VALAPR) AS DECIMAL(18,2)) AS SUM_NU_PA_VALAPR,
            CAST(SUM(NU_VPA_TOT) AS DECIMAL(18,2)) AS SUM_NU_VPA_TOT,
            SUM(NU_PA_TOT) AS SUM_NU_PA_TOT
        FROM SIA.TB_PA
        WHERE CO_PA_CMP >= '202201'
        AND CO_PA_CMP < '202301'
        AND NU_PA_VALPRO >= 0 -- Atenção Básica vem com valor zerado, mas precisamos das quantidades
        AND NU_PA_VALAPR >= 0
        GROUP BY
            CO_PA_CMP, CO_PA_CODUNI, CO_PA_GESTAO, CO_PA_TPUPS,
            CO_PA_TPFIN, CO_PA_TIPPRE, CO_PA_CBOCOD, CO_PA_PROC_ID, CO_NATUREZA_JUR
    """
    try:
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            for chunk in pd.read_sql(query, connection, chunksize = 4_000_000):
                yield chunk

    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise