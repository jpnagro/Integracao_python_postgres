from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
from datetime import date
import json
import pandas as pd
import pandas.io.sql as sqlio
import solicitacao
import operacao
import time

# Criar API
app_main = FastAPI()

# Converter Lista de Objetos em DataFrame
def convert_obj_to_df(lista):
    for i in range(len(lista)):
        lista[i] = lista[i].__dict__
    lista = pd.DataFrame(lista)
    return lista

# Verificar status do banco de dados
db_active = False
try:
    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password="8283", host="localhost")
    cur = conn.cursor()
    db_active = True

    # Lendo o arquivo SQL e convertendo-o de DataFrame para JSON
    df_solicitacao = sqlio.read_sql_query(
        "SELECT * FROM solicitacao;", conn)
    result_solicitacao = df_solicitacao.to_dict(orient="records")
    js_solicitacao = json.dumps(
        result_solicitacao, indent=4, sort_keys=True, default=str)
    dados_solicitacao = json.loads(js_solicitacao)

    df_operacao = sqlio.read_sql_query("SELECT * FROM operacao;", conn)
    result_operacao = df_operacao.to_dict(orient='records')
    js_operacao = json.dumps(result_operacao, indent=4,
                             sort_keys=True, default=str)
    dados_operacao = json.loads(js_operacao)

    @app_main.get('/')
    def inicio():
        return 'Banco de dados integrado com sucesso!'

except:
    # Criar dados para teste caso houver falha no banco de dados
    dados_solicitacao = [
        solicitacao.Solicitacao(taxid='84508744834', activity='Suino', experience=True, state='MG', city='Uberaba',
                                value=74000.20, installments=5, requestdate=2021/1/5, source='Lucas'),
        solicitacao.Solicitacao(taxid='11076255647', activity='Bovino', experience=True, state='GO', city='Pirenopolis',
                                value=54000.00, installments=8, requestdate=2021/2/9, source='Ana'),
        solicitacao.Solicitacao(taxid='02350255859', activity='Suino', experience=False, state='MG', city='Uberlandia',
                                value=150000.00, installments=20, requestdate=2021/3/15, source='Mateus'),
        solicitacao.Solicitacao(taxid='41462473652', activity='Soja', experience=True, state='SP', city='Araraquara',
                                value=82500.70, installments=11, requestdate=2021/4/7, source='Joao'),
        solicitacao.Solicitacao(taxid='30041383796', activity='Batata', experience=False, state='TO', city='Palmas',
                                value=170000.00, installments=10, requestdate=2021/5/14, source='Mateus'),
        solicitacao.Solicitacao(taxid='47584301363', activity='Milho', experience=False, state='TO', city='Arraias',
                                value=87000.00, installments=16, requestdate=2021/6/10, source='Ana'),
        solicitacao.Solicitacao(taxid='14843132403', activity='Soja', experience=False, state='MG', city='Uberlandia',
                                value=250000.00, installments=20, requestdate=2021/7/11, source='Leticia'),
        solicitacao.Solicitacao(taxid='82513250648', activity='Aves', experience=True, state='RJ', city='Niteroi',
                                value=120000.00, installments=9, requestdate=2021/8/4, source='Leticia'),
        solicitacao.Solicitacao(taxid='04003766369', activity='Aves', experience=True, state='MG', city='Betim',
                                value=68400.00, installments=5, requestdate=2021/9/10, source='Ana'),
        solicitacao.Solicitacao(taxid='58733021546', activity='Suino', experience=False, state='SP', city='Americana',
                                value=110000.00, installments=7, requestdate=2021/10/16, source='Lucas'),
        solicitacao.Solicitacao(taxid='38416542589', activity='Soja', experience=False, state='MG', city='Uberlandia',
                                value=180000.00, installments=12, requestdate=2021/10/25, source='Leticia')
    ]
    dados_operacao = [
        operacao.Operacao(taxid='84508744834', valueasked=74000.20, value=56000.00, installments=5, interestrate=5.5,
                          approvaldate=2021/1/5, releasedate=2021/1/14, state='MG', city='Uberaba'),
        operacao.Operacao(taxid='11076255647', valueasked=54000.00, value=54000.00, installments=8, interestrate=7.2,
                          approvaldate=2021/2/14, releasedate=2021/2/17, state='GO', city='Pirenopolis'),
        operacao.Operacao(taxid='02350255859', valueasked=150000.00, value=120000.00, installments=20, interestrate=9.7,
                          approvaldate=2021/3/21, releasedate=2021/3/30, state='MG', city='Uberlandia'),
        operacao.Operacao(taxid='41462473652', valueasked=82500.70, value=82500.70, installments=11, interestrate=8.9,
                          approvaldate=2021/4/15, releasedate=2021/4/23, state='SP', city='Araraquara'),
        operacao.Operacao(taxid='30041383796', valueasked=170000.00, value=148000.00, installments=10, interestrate=7.2,
                          approvaldate=2021/5/18, releasedate=2021/5/26, state='TO', city='Palmas'),
        operacao.Operacao(taxid='47584301363', valueasked=87000.00, value=87000.00, installments=16, interestrate=9.7,
                          approvaldate=2021/6/14, releasedate=2021/6/20, state='TO', city='Arraias'),
        operacao.Operacao(taxid='14843132403', valueasked=250000.00, value=195000.00, installments=20, interestrate=9.7,
                          approvaldate=2021/7/16, releasedate=2021/7/29, state='MG', city='Uberlandia'),
        operacao.Operacao(taxid='82513250648', valueasked=120000.00, value=120000.00, installments=9, interestrate=7.2,
                          approvaldate=2021/8/10, releasedate=2021/8/15, state='RJ', city='Niteroi'),
        operacao.Operacao(taxid='04003766369', valueasked=68400.00, value=45000.00, installments=5, interestrate=5.5,
                          approvaldate=2021/9/14, releasedate=2021/9/21, state='MG', city='Betim'),
        operacao.Operacao(taxid='58733021546', valueasked=110000.00, value=110000.00, installments=7, interestrate=7.2,
                          approvaldate=2021/10/23, releasedate=2021/10/29, state='SP', city='Americana'),
        operacao.Operacao(taxid='38416542589', valueasked=180000.00, value=160000.00, installments=12, interestrate=8.9,
                          approvaldate=2021/10/30, releasedate=2021/11/5, state='MG', city='Uberlandia')
    ]
    df_solicitacao = convert_obj_to_df(dados_solicitacao)
    df_operacao = convert_obj_to_df(dados_operacao)

    @app_main.get('/')
    def inicio():
        return 'Falha ao integrar banco de dados.'

# Rota dos dados de solicitações
@app_main.get('/solicitacoes')
def get_solicitacoes():
    if dados_solicitacao == []:
        return 'Não há registro de solicitação'
    else:
        return dados_solicitacao

# Rota dos dados de operações


@app_main.get('/operacoes')
def get_operacao():
    if dados_operacao == []:
        return 'Não há registro de operação'
    else:
        return dados_operacao


# Métricas
df_operacao['valueinterested'] = df_operacao['value'] * \
    (df_operacao['interestrate']/100)
df_operacao['month'] = pd.DatetimeIndex(df_operacao['approvaldate']).month
freq_estado = df_operacao['state'].value_counts().to_dict()
valorpedido_estado = df_operacao.groupby(
    'state')['valueasked'].sum().to_dict()
valoraprovado_estado = df_operacao.groupby(
    'state')['value'].sum().to_dict()
valorrecebido_estado = df_operacao.groupby(
    'state')['valueinterested'].sum().to_dict()
data_aprovada = df_operacao.groupby('month')['value'].sum().to_dict()
top_func_qtde = df_solicitacao['source'].value_counts().to_dict()
top_func_valor = df_solicitacao.groupby('source')['value'].sum().to_dict()
atvd_cliente_qtd = df_solicitacao['activity'].value_counts().to_dict()
atvd_cliente_valor = df_solicitacao.groupby(
    'activity')['value'].sum().to_dict()
exp_qtd = df_solicitacao['experience'].value_counts().to_dict()
exp_value = df_solicitacao.groupby('experience')['value'].sum().to_dict()

# Rota das métricas
@app_main.get('/metricas')
def get_metricas():
    return freq_estado, valorpedido_estado, valoraprovado_estado, valorrecebido_estado, data_aprovada, top_func_qtde, top_func_valor, atvd_cliente_qtd, atvd_cliente_valor, exp_qtd, exp_value

# Encerrar o banco de dados
if db_active:
    cur.close()
    conn.close()
