import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
import cx_Oracle
import os

@model(
    "raw.sia__tb_pa",
    kind = dict(name=ModelKindName.INCREMENTAL_BY_TIME_RANGE, time_column="CO_PA_CMP"),
    start="2022-01-01",
    end="2022-12-31",
    columns={
        # Campos de identificação do estabelecimento
        "CO_PA_CODUNI": "TEXT",        # Código do Estabelecimento no CNES
        "CO_PA_GESTAO": "TEXT",        # Código da UF + Código do Município do Gestor
        "CO_PA_CONDIC": "TEXT",        # Sigla do Tipo de Gestão
        "CO_PA_UFMUN": "TEXT",         # UF + Código do Município do estabelecimento
        "CO_PA_REGCT": "TEXT",         # Código da Regra Contratual
        "CO_PA_INCOUT": "TEXT",        # Incremento Outros
        "CO_PA_INCURG": "TEXT",        # Incremento Urgência
        "CO_PA_TPUPS": "TEXT",         # Tipo de Estabelecimento
        "CO_PA_TIPPRE": "TEXT",        # Tipo de Prestador
        "CO_PA_MN_IND": "TEXT",        # Estabelecimento Mantido / Individual
        "CO_PA_CNPJCPF": "TEXT",       # CNPJ do Estabelecimento executante
        "CO_PA_CNPJMNT": "TEXT",       # CNPJ da Mantenedora
        "CO_PA_CNPJ_CC": "TEXT",       # CNPJ do Órgão (cessão de crédito)
        
        # Campos de data e procedimento
        "CO_PA_MVM": "TEXT",           # Data de Processamento / Movimento
        "CO_PA_CMP": "TEXT",           # Data da Realização do Procedimento
        "CO_PA_PROC_ID": "TEXT",       # Código do Procedimento Ambulatorial
        "CO_PA_TPFIN": "TEXT",         # Tipo de Financiamento
        "CO_PA_SUBFIN": "TEXT",        # Subtipo de Financiamento
        "CO_PA_NIVCPL": "TEXT",        # Complexidade do Procedimento
        "CO_PA_DOCORIG": "TEXT",       # Instrumento de Registro
        "CO_PA_AUTORIZ": "TEXT",       # Número da APAC/autorização
        
        # Campos do profissional
        "CO_PA_CBOCOD": "TEXT",        # Código da Ocupação na CBO
        
        # Campos de diagnóstico
        "CO_PA_CIDPRI": "TEXT",        # CID Principal
        "CO_PA_CIDSEC": "TEXT",        # CID Secundário
        "CO_PA_CIDCAS": "TEXT",        # CID Causas Associadas
        "CO_PA_CATEND": "TEXT",        # Caráter de Atendimento
        
        # Campos numéricos de produção
        "NU_PA_QTDPRO": "BIGINT",      # Quantidade Produzida
        "NU_PA_QTDAPR": "BIGINT",      # Quantidade Aprovada
        "NU_PA_VALPRO": "DOUBLE",      # Valor Produzido
        "NU_PA_VALAPR": "DOUBLE",      # Valor Aprovado
        
        # Campos de diferença
        "NU_PA_DIF_VAL": "DOUBLE",     # Diferença de Valor
        
        # Campos de valor
        "NU_VPA_TOT": "DOUBLE",        # Valor Unitário Tabela VPA
        "NU_PA_TOT": "DOUBLE",         # Valor Unitário Tabela SIGTAP
        
        # Campos de controle
        "CO_PA_INDICA": "TEXT",        # Indicador situação produção
        "CO_PA_CODOCO": "TEXT",        # Código da Ocorrência
        "CO_PA_FLQT": "TEXT",          # Indicador erro Quantidade
        "CO_PA_FLER": "TEXT",          # Indicador erro APAC
        "CO_PA_FLH": "TEXT",           # Folha
        "CO_PA_SEQ": "TEXT",           # Sequencial
        "CO_PA_REMESSA": "TEXT",       # Código Remessa
        
        # Campos de valor complementar
        "NU_VL_FEDERAL": "DOUBLE",     # Valor Complemento Federal
        "NU_VL_LOCAL": "DOUBLE",       # Valor Complemento Local
        "NU_VL_INCREMENTO": "DOUBLE",  # Valor Incremento
        
        # Campos adicionais
        "CO_SERV_CLASS": "TEXT",       # Código Serviço/Classificação
        "CO_PA_INE": "TEXT",           # Código Nacional de Equipes
        "CO_NATUREZA_JUR": "TEXT",     # Código Natureza Jurídica
    },
    column_descriptions={
        "CO_PA_CODUNI": "Código do Estabelecimento no CNES",
        "CO_PA_GESTAO": "Código da UF + Código do Município do Gestor, ou UF0000 se Gestão Estadual",
        "CO_PA_CONDIC": "Sigla do Tipo de Gestão",
        "CO_PA_UFMUN": "UF + Código do Município onde está o estabelecimento",
        "CO_PA_REGCT": "Código da Regra Contratual",
        "CO_PA_INCOUT": "Incremento Outros",
        "CO_PA_INCURG": "Incremento Urgência",
        "CO_PA_TPUPS": "Tipo de Estabelecimento",
        "CO_PA_TIPPRE": "Tipo de Prestador",
        "CO_PA_MN_IND": "Estabelecimento Mantido / Individual",
        "CO_PA_CNPJCPF": "CNPJ do Estabelecimento executante",
        "CO_PA_CNPJMNT": "CNPJ da Mantenedora ou zeros",
        "CO_PA_CNPJ_CC": "CNPJ do Órgão que recebeu por cessão de crédito ou zeros",
        "CO_PA_MVM": "Data de Processamento / Movimento (AAAAMM)",
        "CO_PA_CMP": "Data da Realização do Procedimento / Competência (AAAAMM)",
        "CO_PA_PROC_ID": "Código do Procedimento Ambulatorial",
        "CO_PA_TPFIN": "Tipo de Financiamento da produção",
        "CO_PA_SUBFIN": "Subtipo de Financiamento da produção",
        "CO_PA_NIVCPL": "Complexidade do Procedimento",
        "CO_PA_DOCORIG": "Instrumento de Registro",
        "CO_PA_AUTORIZ": "Número da APAC ou número de autorização do BPA-I",
        "CO_PA_CBOCOD": "Código da Ocupação na CBO",
        "CO_PA_CIDPRI": "CID Principal",
        "CO_PA_CIDSEC": "CID Secundário",
        "CO_PA_CIDCAS": "CID Causas Associadas",
        "CO_PA_CATEND": "Caráter de Atendimento",
        "NU_PA_QTDPRO": "Quantidade Produzida (APRESENTADA)",
        "NU_PA_QTDAPR": "Quantidade Aprovada do procedimento",
        "NU_PA_VALPRO": "Valor Produzido (APRESENTADO)",
        "NU_PA_VALAPR": "Valor Aprovado do procedimento",
        "NU_PA_DIF_VAL": "Diferença do Valor Unitário",
        "NU_VPA_TOT": "Valor Unitário do Procedimento da Tabela VPA",
        "NU_PA_TOT": "Valor Unitário do Procedimento da Tabela SIGTAP",
        "CO_PA_INDICA": "Indicativo de situação (0=não aprovado; 5=aprovado total; 6=aprovado parcial)",
        "CO_PA_CODOCO": "Código de Ocorrência",
        "CO_PA_FLQT": "Indicador de erro de Quantidade Produzida",
        "CO_PA_FLER": "Indicador de erro de corpo da APAC",
        "CO_PA_FLH": "Folha",
        "CO_PA_SEQ": "Sequencial",
        "CO_PA_REMESSA": "Código Remessa",
        "NU_VL_FEDERAL": "Valor do Complemento Federal",
        "NU_VL_LOCAL": "Valor do Complemento Local",
        "NU_VL_INCREMENTO": "Valor do Incremento",
        "CO_SERV_CLASS": "Código do Serviço Especializado / Classificação CBO",
        "CO_PA_INE": "Código de Identificação Nacional de Equipes",
        "CO_NATUREZA_JUR": "Código da Natureza Jurídica"
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
            CO_PA_CODUNI, CO_PA_GESTAO, CO_PA_CONDIC, CO_PA_UFMUN, 
            CO_PA_REGCT, CO_PA_INCOUT, CO_PA_INCURG, CO_PA_TPUPS, 
            CO_PA_TIPPRE, CO_PA_MN_IND, CO_PA_CNPJCPF, CO_PA_CNPJMNT, 
            CO_PA_CNPJ_CC, CO_PA_MVM, CO_PA_CMP, CO_PA_PROC_ID, 
            CO_PA_TPFIN, CO_PA_SUBFIN, CO_PA_NIVCPL, CO_PA_DOCORIG, 
            CO_PA_AUTORIZ, CO_PA_CBOCOD, CO_PA_CIDPRI, CO_PA_CIDSEC, 
            CO_PA_CIDCAS, CO_PA_CATEND, NU_PA_QTDPRO, NU_PA_QTDAPR, 
            NU_PA_VALPRO, NU_PA_VALAPR, NU_PA_DIF_VAL, NU_VPA_TOT, 
            NU_PA_TOT, CO_PA_INDICA, CO_PA_CODOCO, CO_PA_FLQT, 
            CO_PA_FLER, CO_PA_FLH, CO_PA_SEQ, CO_PA_REMESSA, 
            NU_VL_FEDERAL, NU_VL_LOCAL, NU_VL_INCREMENTO, 
            CO_SERV_CLASS, CO_PA_INE, CO_NATUREZA_JUR
        FROM SIA.TB_PA
        WHERE CO_PA_CMP >= '202201'
        AND CO_PA_CMP <= '202212'
    """
    try:
        with cx_Oracle.connect(username, password, dsn, encoding="UTF-8") as connection:
            for chunk in pd.read_sql(query, connection, chunksize = 4_000_000):
                yield chunk

    except cx_Oracle.Error as error:
        print("Error while connecting to Oracle Database:", error)
        raise