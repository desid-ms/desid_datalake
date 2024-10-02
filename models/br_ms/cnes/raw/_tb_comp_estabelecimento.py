import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw_cnes.tb_comp_estabelecimento",
    kind = dict (name = ModelKindName.INCREMENTAL_BY_UNIQUE_KEY, unique_key = "CO_UNIDADE"),
    columns={
        "CO_UNIDADE": "TEXT",
        "CO_CNES": "TEXT",
        "TP_PFPJ": "TEXT",
        "CO_SIASUS": "TEXT",
        "NIVEL_DEP": "TEXT",
        "NO_RAZAO_SOCIAL": "TEXT",
        "NO_FANTASIA": "TEXT",
        "NO_LOGRADOURO": "TEXT",
        "NU_ENDERECO": "TEXT",
        "NO_BAIRRO": "TEXT",
        "CO_CEP": "TEXT",
        "CO_REGIAO_SAUDE": "TEXT",
        "CO_DISTRITO_SANITARIO": "TEXT",
        "CO_DISTRITO_ADMINISTRATIVO": "TEXT",
        "NU_TELEFONE": "TEXT",
        "NU_FAX": "TEXT",
        "NO_EMAIL": "TEXT",
        "NU_CPF": "TEXT",
        "NU_CNPJ": "TEXT",
        "CO_ATIVIDADE": "TEXT",
        "CO_RETENCAO_TRIBUTARIA": "TEXT",
        "CO_CLIENTELA": "TEXT",
        "NU_ALVARA": "TEXT",
        "DT_EXPEDICAO": "TEXT",  
        "TP_ORGAO_EXPEDIDOR": "TEXT",
        "TP_AVALIACAO": "TEXT",
        "TP_CLASSE_AVALIACAO": "TEXT",
        "TP_UNIESP": "TEXT",
        "TP_VINCULO_SUS": "TEXT",
        "TP_TERCEIRO_SIH": "TEXT",
        "CO_ESTADO_GESTOR": "TEXT",
        "CO_MUNICIPIO_GESTOR": "TEXT",
        "ST_DIALIASE": "TEXT",
        "CO_SIASUS1": "TEXT",
        "CO_SIASUS2": "TEXT",
        "CO_SIASUS3": "TEXT",
        "CO_SIASUS4": "TEXT",
        "CO_SIASUS5": "TEXT",
        "DT_ATUALIZACAO": "TEXT",  
        "CO_USUARIO": "TEXT",
        "CO_NATUREZA_ORGANIZACAO": "TEXT",
        "CO_NIVEL_HIERARQUIA": "TEXT",
        "CO_ESFERA_ADMINISTRATIVA": "TEXT",
        "TP_UNIDADE": "TEXT",
        "NU_CNPJ_MANTENEDORA": "TEXT",
        "CO_TURNO_ATENDIMENTO": "TEXT",
        "CO_TIPO_UNIDADE": "TEXT",
        "NO_COMPLEMENTO": "TEXT",
        "CO_MICRO_REGIAO": "TEXT",
        "DT_ATUALIZACAO_ORIGEM": "TEXT",  
        "DT_CMTP_INICIO": "TEXT",  
        "DT_CMTP_FIM": "TEXT",  
        "NU_SEQ_PROCESSO": "INT",
        "TP_GESTAO": "TEXT",
        "NO_FANTASIA_ABREV": "TEXT",
        "TP_PRESTADOR": "TEXT",
        "CO_CPFDIRETORCLN": "TEXT",
        "REG_DIRETORCLN": "TEXT",
        "ST_AVALIACAO_PNASS": "TEXT",
        "DT_AVALIACAO_PNASS": "TEXT",  
        "DT_CREDITACAO": "TEXT",  
        "CMPT_INICIAL": "TEXT",
        "CMPT_FINAL": "TEXT",
        "CO_MOTIVO_DESAB": "TEXT",
        "ST_ADESAO_FILANTROP": "TEXT",
        "CMPT_VIGENTE": "TEXT",
        "NO_URL": "TEXT",
        "NU_LATITUDE": "TEXT",
        "NU_LONGITUDE": "TEXT",
        "DT_ATU_GEO": "TEXT",  
        "NO_USUARIO_GEO": "TEXT",
        "CO_NATUREZA_JUR": "TEXT",
        "ST_CONTRATO_SUS": "TEXT",
        "DT_VAL_LIC_SANI": "TEXT",  
        "TP_LIC_SANI": "TEXT",
        "TP_ESTAB_SEMPRE_ABERTO": "TEXT",
        "ST_GERACREDITO_GERENTE_SGIF": "TEXT",
        "ST_CONEXAO_INTERNET": "TEXT",
        "CO_TIPO_ESTABELECIMENTO": "TEXT",
        "CO_ATIVIDADE_PRINCIPAL": "TEXT",
        "ST_CONTRATO_FORMALIZADO": "TEXT",
        "CO_TIPO_ABRANGENCIA": "TEXT",
        "ST_COWORKING": "TEXT"}
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
    query = "SELECT * FROM CNES.TB_AUX_ESTABELECIMENTO"

    try:
        # Establish the connection
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            df = pd.read_sql(query, connection)
        return df
    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise