import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw.sih__tb_reduz_rejeit",
    columns={
        "ST_SITUACAO": "TEXT",
        "CO_IDENT": "TEXT",
        "NU_ESPECIALIDADE": "TEXT",
        "NU_AIH": "TEXT",
        "NU_SEQ_AIH5": "TEXT",
        "DT_CMPT": "TEXT",
        "CO_OE_GESTOR": "TEXT",
        "CO_CNES": "TEXT",
        "NU_MUN_HOSP": "TEXT",
        "DT_INTERNACAO": "TEXT",
        "DT_SAIDA": "TEXT",
        "CO_PROC_SOLICITADO": "TEXT",
        "CO_PROC_REALIZADO": "TEXT",
        "CO_CAR_INTERNACAO": "TEXT",
        "CO_MOT_SAIDA": "TEXT",
        "CO_DIAG_PRI": "TEXT",
        "CO_DIAG_SEC": "TEXT",
        "CO_DIAG_COMP": "TEXT",
        "CO_DIAG_OBITO": "TEXT",
        "NO_PACIENTE_NOME": "TEXT",
        "DT_PACIENTE_NASCIMENTO": "TEXT",
        "CO_PACIENTE_SEXO": "TEXT",
        "CO_PACIENTE_NACIONALIDADE": "TEXT",
        "DS_PACIENTE_LOGR": "TEXT",
        "NU_PACIENTE_LOGR_NUMERO": "TEXT",
        "DS_PACIENTE_LOGR_COMPL": "TEXT",
        "NU_PACIENTE_LOGR_MUNICIPIO": "TEXT",
        "CO_PACIENTE_LOGR_CEP": "TEXT",
        "CO_ACDTRAB_CBOR": "TEXT",
        "CO_ACDTRAB_CNAER": "TEXT",
        "CO_ACDTRAB_VINC_PREV": "TEXT",
        "NU_PARTO_NUM_PRENATAL": "TEXT",
        "QT_LAQVAS_QTD_FILHOS": "TEXT",
        "CO_LAQVAS_GRAU_INSTRUC": "TEXT",
        "CO_LAQVAS_CID_INDICACAO": "TEXT",
        "CO_LAQVAS_MET_CONTRACEP1": "TEXT",
        "CO_LAQVAS_MET_CONTRACEP2": "TEXT",
        "ST_LAQVAS_GESTACAO_RISCO": "TEXT",
        "ST_DUPLICIDADE": "TEXT",
        "ST_BLOQUEIO": "TEXT",
        "CO_MOT_BLOQ": "TEXT",
        "TP_GESTOR_IDENT": "TEXT",
        "NU_GESTOR_DOC": "TEXT",
        "CO_CONTRATO": "TEXT",
        "QT_DIARIAS": "DOUBLE",
        "QT_DIARIAS_UTI": "DOUBLE",
        "QT_DIARIAS_UI": "DOUBLE",
        "CO_COMPLEXIDADE": "TEXT",
        "CO_FINANCIAMENTO": "TEXT",
        "CO_TIPO_FAEC": "TEXT",
        "QT_DIARIA_AC": "DOUBLE",
        "NU_NAT": "TEXT",
        "CO_GESTAO": "TEXT",
        "ST_IND_VDRL": "TEXT",
        "CO_IDADE": "TEXT",
        "NU_IDADE": "TEXT",
        "NU_MORTE": "DOUBLE",
        "ST_MARCA_UTI": "TEXT",
        "NU_VAL_TOT": "DOUBLE",
        "NU_VAL_UTI": "DOUBLE",
        "NU_CNPJ_MANTENEDORA": "TEXT",
        "NU_CNPJ": "TEXT",
        "CO_PACIENTE_RACA_COR": "TEXT",
        "CO_ETNIA": "TEXT",
        "NU_VAL_SADT": "DOUBLE",
        "NU_VAL_SP": "DOUBLE",
        "CO_SEQ": "DOUBLE",
        "NO_ARQ_REMESSA": "TEXT",
        "QT_DIAS_PERM": "DOUBLE",
        "NU_NATJUR": "TEXT",
        "DS_AUDIT_JUST": "TEXT",
        "DS_AUDIT_SISAIH01_JUST": "TEXT",
        "NU_VAL_SH_FED": "DOUBLE",
        "NU_VAL_SP_FED": "DOUBLE",
        "NU_VAL_SH_GES": "DOUBLE",
        "NU_VAL_SP_GES": "DOUBLE",
        "CO_SOL_LIB": "TEXT",
        "NU_VAL_UCI": "DOUBLE",
        "ST_MARCA_UCI": "TEXT",
        "CO_DIAG_SEC_1": "TEXT",
        "TP_DIAG_SEC_1_CLASS": "TEXT",
        "CO_DIAG_SEC_2": "TEXT",
        "TP_DIAG_SEC_2_CLASS": "TEXT",
        "CO_DIAG_SEC_3": "TEXT",
        "TP_DIAG_SEC_3_CLASS": "TEXT",
        "CO_DIAG_SEC_4": "TEXT",
        "TP_DIAG_SEC_4_CLASS": "TEXT",
        "CO_DIAG_SEC_5": "TEXT",
        "TP_DIAG_SEC_5_CLASS": "TEXT",
        "CO_DIAG_SEC_6": "TEXT",
        "TP_DIAG_SEC_6_CLASS": "TEXT",
        "CO_DIAG_SEC_7": "TEXT",
        "TP_DIAG_SEC_7_CLASS": "TEXT",
        "CO_DIAG_SEC_8": "TEXT",
        "TP_DIAG_SEC_8_CLASS": "TEXT",
        "CO_DIAG_SEC_9": "TEXT",
        "TP_DIAG_SEC_9_CLASS": "TEXT",
        "NU_PACIENTE_NUMERO_CNS": "TEXT",
        "ST_PACIENTE_DADOS_VLDDS_CNS": "TEXT",
        "NU_PACIENTE_NUMERO_CPF": "TEXT"
    },
    column_descriptions={
        "ST_SITUACAO": "SITUACAO 0-APROVADA 1-REJEITADA 9-REJEITADA NA IMPORTACAO",
        "CO_IDENT": "IDENTIFICACAO DO TIPO DE AIH 1-NORMAL 3- CONTINUACAO 4-REGISTRO CIVIL 5-LONGA PERMANENCIA",
        "CO_TIPO_FAEC": "CODIGO DO TIPO DE FAEC",
        "QT_DIARIA_AC": "NUMERO DE DIARIAS DE ACOMPANHANTE",
        "NU_NAT": "NATUREZA DO HOSPITAL",
        "CO_GESTAO": "CODIGO DE GESTAO",
        "ST_IND_VDRL": "INDICADOR DE VDRL",
        "CO_IDADE": "CODIGO DA IDADE",
        "NU_IDADE": "NUMERO DA IDADE",
        "NU_MORTE": "CODIGO DE MORTE",
        "ST_MARCA_UTI": "CODIGO DO TIPO DE UTI",
        "NU_VAL_TOT": "VALOR TOTAL DA AIH",
        "NU_VAL_UTI": "VALOR DE UTI",
        "NU_CNPJ_MANTENEDORA": "NUMERO DO CNPJ DA MANTENEDORA",
        "NU_CNPJ": "NUMERO DO CNPJ DO HOSPITAL",
        "CO_PACIENTE_RACA_COR": "RACA/COR DO PACIENTE",
        "CO_ETNIA": "ETNIA DO PACIENTE",
        "NU_VAL_SADT": "VALOR SADT",
        "NU_VAL_SP": "VALOR SP",
        "DS_AUDIT_JUST": "justificativa auditoria",
        "DS_AUDIT_SISAIH01_JUST": "justificativa auditoria sisaih",
        "NU_VAL_SH_FED": "complemento valores sh federal",
        "NU_VAL_SP_FED": "complemento valores sp federal",
        "NU_VAL_SH_GES": "complemento valores sh gestor",
        "NU_VAL_SP_GES": "complemento valores sp gestor",
        "CO_DIAG_SEC_1": "Código do 1º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_1_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_2": "Código do 2º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_2_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_3": "Código do 3º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_3_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_4": "Código do 4º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_4_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_5": "Código do 5º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_5_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_6": "Código do 6º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_6_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_7": "Código do 7º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_7_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_8": "Código do 8º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_8_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "CO_DIAG_SEC_9": "Código do 9º diagnóstico secundário (CID-10) -  (ver  TU_CID)",
        "TP_DIAG_SEC_9_CLASS": "0-Quando campo anterior em branco, 1-PREEXISTENTE, 2-ADQUIRIDO",
        "NU_PACIENTE_NUMERO_CNS": "Número do cartão nacional de saúde (CNS) do paciente",
        "ST_PACIENTE_DADOS_VLDDS_CNS": "Informa se os dados do paciente foram validados no sistema do Cartão Nacional de Saúde ( 0-NAO, 1-SIM )",
        "NU_PACIENTE_NUMERO_CPF": "Número do CPF do paciente"
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
        WITH combined_data AS (
            SELECT *
            FROM AIH.TB_REDUZ
            WHERE SUBSTR(dt_saida, 1, 4) = '2022'
            UNION ALL
            SELECT *
            FROM AIH.TB_REJEIT
            WHERE SUBSTR(dt_saida, 1, 4) = '2022'
        )
        SELECT *
        FROM combined_data
        ORDER BY
            NU_AIH,
            ST_SITUACAO,
            DT_SAIDA,
            DT_CMPT
     """

    try:
        # # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            for chunk in pd.read_sql(query, connection, chunksize =  4_000_000):
                yield chunk

    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise


# select nu_aih, left(dt_saida, 6) as dt_exercicio, count(*) as count from raw.sih__tb_reduz_rejeit where st_situacao=0 and co_ident=5
# ‣ group by nu_aih, dt_exercicio order by count desc;

# select st_situacao, co_ident, nu_aih, count(*) as count from raw.sih__tb_reduz_rejeit where st_situacao=0 and co_ident = 1 group by all order by count desc;