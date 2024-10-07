import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw_sia.tb_pa",
    columns={
        # "CO_PA_REMESSA": "TEXT",
        # "CO_PA_CNSPAC": "TEXT",
        # "CO_PA_NMPAC": "TEXT",
        # "CO_PA_CODIDADE": "TEXT",
        # "CO_PA_DTNASC": "TEXT",
        "CO_PA_NUIDADE": "TEXT",
        "CO_PA_ETNIA": "TEXT",
        # "NU_VL_FEDERAL": "DOUBLE",
        # "NU_VL_LOCAL": "DOUBLE",
        # "NU_VL_INCREMENTO": "DOUBLE",
        # "CO_SERV_CLASS": "TEXT",
        # "CO_PA_INE": "TEXT",
        "CO_NATUREZA_JUR": "TEXT",
        "CO_PA_CODUNI": "TEXT",
        "CO_PA_GESTAO": "TEXT",
        # "CO_PA_CONDIC": "TEXT",
        "CO_PA_UFMUN": "TEXT",
        # "CO_PA_REGCT": "TEXT",
        # "CO_PA_INCOUT": "TEXT",
        # "CO_PA_INCURG": "TEXT",
        "CO_PA_TPUPS": "TEXT",
        "CO_PA_TIPPRE": "TEXT",
        # "CO_PA_MN_IND": "TEXT",
        # "CO_PA_CNPJCPF": "TEXT",
        # "CO_PA_CNPJMNT": "TEXT",
        # "CO_PA_CNPJ_CC": "TEXT",
        "CO_PA_MVM": "TEXT",
        "CO_PA_CMP": "TEXT",
        "CO_PA_PROC_ID": "TEXT",
        "CO_PA_TPFIN": "TEXT",
        # "CO_PA_SUBFIN": "TEXT",
        # "CO_PA_NIVCPL": "TEXT",
        # "CO_PA_DOCORIG": "TEXT",
        # "CO_PA_AUTORIZ": "TEXT",
        # "CO_PA_CNSMED": "TEXT",
        "CO_PA_CBOCOD": "TEXT",
        # "CO_PA_MOTSAI": "TEXT",
        # "CO_PA_OBITO": "TEXT",
        # "CO_PA_ENCERR": "TEXT",
        # "CO_PA_PERMAN": "TEXT",
        # "CO_PA_ALTA": "TEXT",
        # "CO_PA_TRANSF": "TEXT",
        "CO_PA_CIDPRI": "TEXT",
        # "CO_PA_CIDSEC": "TEXT",
        # "CO_PA_CIDCAS": "TEXT",
        # "CO_PA_CATEND": "TEXT",
        # "CO_PA_IDADE": "TEXT",
        # "NU_IDADEMIN": "INT",
        # "NU_IDADEMAX": "INT",
        # "CO_PA_FLIDADE": "TEXT",
        "CO_PA_SEXO": "TEXT",
        "CO_PA_RACACOR": "TEXT",
        "CO_PA_MUNPCN": "TEXT",
        "NU_PA_QTDPRO": "INT",
        "NU_PA_QTDAPR": "INT",
        "NU_PA_VALPRO": "DOUBLE",
        "NU_PA_VALAPR": "DOUBLE",
        # "CO_PA_UFDIF": "TEXT",
        # "CO_PA_MNDIF": "TEXT",
        # "NU_PA_DIF_VAL": "DOUBLE",
        "NU_VPA_TOT": "DOUBLE",
        "NU_PA_TOT": "DOUBLE",
        # "CO_PA_INDICA": "TEXT",
        # "CO_PA_CODOCO": "TEXT",
        # "CO_PA_FLQT": "TEXT",
        # "CO_PA_FLER": "TEXT",
        # "CO_PA_FLH": "TEXT",
        # "CO_PA_SEQ": "TEXT",
    },
    column_descriptions={
        # "CO_PA_REMESSA": "CÓDIGO REMESSA",
        # "CO_PA_CNSPAC": "CNS DO PACIENTE",
        # "CO_PA_NMPAC": "NOME DO PACIENTE",
        # "CO_PA_CODIDADE": "CODIGO IDADE DO PACIENTE",
        # "CO_PA_DTNASC": "DATA NASCIMENTO PACIENTE",
        "CO_PA_NUIDADE": "QUANTIDADE IDADE DO PACIENTE",
        "CO_PA_ETNIA": "Código de etnia",
        # "CO_PA_INE": "CÓDIGO DE IDENTIFICAÇÃO NACIONAL DE EQUIPES",
        "CO_NATUREZA_JUR": "CÓDIGO DA NATUREZA JURIDICA",
        "CO_PA_CODUNI": "CÓDIGO CNES",
        "CO_PA_GESTAO": "CÓDIGO IBGE",
        # "CO_PA_CONDIC": "CONDIÇÃO",
        "CO_PA_UFMUN": "CÓDIGO DA UF",
        # "CO_PA_REGCT": "REGRA CONTRATUAL",
        # "CO_PA_INCOUT": "INCREMENTO OUTROS",
        # "CO_PA_INCURG": "INCREMENTO URGÊNCIA",
        "CO_PA_TPUPS": "TIPO UNIDADE",
        "CO_PA_TIPPRE": "TIPO DE PRESTADOR",
        # "CO_PA_MN_IND": "MANTIDO OU INDIVIDUAL",
        # "CO_PA_CNPJCPF": "CNPJ OU CPF",
        # "CO_PA_CNPJMNT": "CNPJ MANTENEDORA",
        # "CO_PA_CNPJ_CC": "CONTA CORRENTE",
        "CO_PA_MVM": "MOVIMENTO",
        "CO_PA_CMP": "COMPETÊNCIA",
        "CO_PA_PROC_ID": "CÓDIGO PROCEDIMENTO",
        "CO_PA_TPFIN": "TIPO FINANCIAMENTO",
        # "CO_PA_SUBFIN": "SUBTIPO FINANCIAMENTO",
        # "CO_PA_NIVCPL": "NIVEL COMPLEXIDADE",
        # "CO_PA_DOCORIG": "DOCUMENTO ORIGEM",
        # "CO_PA_AUTORIZ": "NÚMERO AUTORIZAÇÃO",
        # "CO_PA_CNSMED": "CNS DO MÉDICO RESPONSÁVEL",
        "CO_PA_CBOCOD": "CBO DO MÉDICO RESPONSÁVEL",
        # "CO_PA_MOTSAI": "MOTIVO DE SAÍDA",
        # "CO_PA_OBITO": "MOTIVO DE SAÍDA - ÓBITO",
        # "CO_PA_ENCERR": "MOTIVO DE SAÍDA - ENCERRAMENTO",
        # "CO_PA_PERMAN": "MOTIVO DE SAÍDA - PERMANÊNCIA",
        # "CO_PA_ALTA": "MOTIVO DE SAÍDA - ALTA",
        # "CO_PA_TRANSF": "MOTIVO DE SAÍDA - TRANSFERÊNCIA",
        "CO_PA_CIDPRI": "CID PRINCIPAL",
        # "CO_PA_CIDSEC": "CID SECUNDÁRIO",
        # "CO_PA_CIDCAS": "CID CASUAL",
        # "CO_PA_CATEND": "ATENDIMENTO",
        # "CO_PA_IDADE": "IDADE EM ANOS",
        # "NU_IDADEMIN": "IDADE MÍNIMA PROCEDIMENTO",
        # "NU_IDADEMAX": "IDADE MÁXIMA PROCEDIMENTO",
        # "CO_PA_FLIDADE": "FLAG IDADE",
        "CO_PA_SEXO": "SEXO",
        "CO_PA_RACACOR": "RAÇA",
        "CO_PA_MUNPCN": "MUNICÍPIO RESIDÊNCIA",
        "NU_PA_QTDPRO": "QUANTIDADE PRODUZIDA",
        "NU_PA_QTDAPR": "QUANTIDADE APROVADA",
        "NU_PA_VALPRO": "VALOR PRODUZIDO",
        "NU_PA_VALAPR": "VALOR APROVADO",
        # "CO_PA_UFDIF": "UF RESIDENCIA DIFERENTE DA UF DA UNIDADE",
        # "CO_PA_MNDIF": "MUNICÍPIO RESIDENCIA DIFERENTE DO MUNICÍPIO DA UNIDADE",
        # "NU_PA_DIF_VAL": "DIFERENÇA DE VALOR",
        "NU_VPA_TOT": "VALOR TOTAL",
        "NU_PA_TOT": "VALOR TOTAL",
        # "CO_PA_INDICA": "INDICADOR SA SITUAÇÃO DA PRODUÇÃO",
        # "CO_PA_CODOCO": "CÓDIGO DA OCORRÊNCIA",
        # "CO_PA_FLQT": "FLAG SITUAÇÃO",
        # "CO_PA_FLER": "FLAG ERRO",
        # "CO_PA_FLH": "FOLHA",
        # "CO_PA_SEQ": "SEQUENCIAL",
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
    password = os.environ['ORACLE_RJPO1DR_PWD']
    host = 'exaccdfdr-scan.saude.gov'
    port = '1521'
    service_name = 'RJPO1DR.saude.gov'
    dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
    query = """
    SELECT
        CO_PA_CMP, CO_PA_MVM, CO_PA_CODUNI, CO_PA_GESTAO, CO_PA_UFMUN, CO_PA_MUNPCN, CO_PA_TPUPS,
        CO_PA_TPFIN, CO_PA_TIPPRE, CO_PA_CBOCOD, CO_PA_SEXO, CO_PA_ETNIA, CO_PA_RACACOR, CO_PA_NUIDADE,
        CO_PA_CIDPRI, CO_PA_PROC_ID, CO_PA_TPFIN, CO_NATUREZA_JUR,
        SUM(NU_PA_QTDPRO) AS SUM_NU_PA_QTDPRO,
        SUM(NU_PA_QTDAPR) AS SUM_NU_PA_QTDAPR,
        SUM(NU_PA_VALPRO) AS SUM_NU_PA_VALPRO,
        SUM(NU_PA_VALAPR) AS SUM_NU_PA_VALAPR,
        SUM(NU_VPA_TOT) AS SUM_NU_VPA_TOT,
        SUM(NU_PA_TOT) AS SUM_NU_PA_TOT
    FROM RAW_SIA__DEV.TB_PA
    WHERE CO_PA_CMP > '202112'
        AND CO_PA_CMP < '202301'
        AND NU_PA_VALPRO > 0
        AND NU_PA_VALAPR > 0
    GROUP BY
        CO_PA_CMP, CO_PA_MVM, CO_PA_CODUNI, CO_PA_GESTAO, CO_PA_UFMUN, CO_PA_MUNPCN, CO_PA_TPUPS,
        CO_PA_TPFIN, CO_PA_TIPPRE, CO_PA_CBOCOD, CO_PA_SEXO, CO_PA_ETNIA, CO_PA_RACACOR, CO_PA_NUIDADE,
        CO_PA_CIDPRI, CO_PA_PROC_ID, CO_PA_TPFIN, CO_NATUREZA_JUR
    -- SELECT 
    --     CO_PA_CMP, CO_PA_MVM, CO_PA_CODUNI, CO_PA_GESTAO, CO_PA_UFMUN, CO_PA_MUNPCN, CO_PA_TPUPS,
    --     CO_PA_TPFIN, CO_PA_TIPPRE, CO_PA_CBOCOD, CO_PA_SEXO, CO_PA_ETNIA, CO_PA_RACACOR, CO_PA_NUIDADE, 
    --     CO_PA_CIDPRI,CO_PA_PROC_ID, CO_PA_TPFIN,NU_PA_QTDPRO, NU_PA_QTDAPR, NU_PA_VALPRO, NU_PA_VALAPR, 
    --     NU_VPA_TOT, NU_PA_TOT, CO_NATUREZA_JUR  
    -- FROM SIA.TB_PA 
    -- WHERE CO_PA_CMP > '202112' AND CO_PA_CMP < '202301' AND NU_PA_VALPRO > 0 AND NU_PA_VALAPR > 0
    """

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise