from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
from datetime import date
import json
import pandas.io.sql as sqlio

# Criar API
app_solicitacao = FastAPI()

dados_solicitacao = []

# Criar model solicitação
class Solicitacao(BaseModel):
    taxid: str
    activity: str
    experience: bool
    state: str
    city: str
    value: float
    installments: int
    requestdate: date
    source: str

# Verificar status do banco de dados
db_active = False
try:
    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password="8283", host="localhost")
    cur = conn.cursor()

    # Lendo o arquivo SQL e convertendo-o de DataFrame para JSON
    bd_solicitacao = sqlio.read_sql_query("SELECT * FROM solicitacao;", conn)
    result_solicitacao = bd_solicitacao.to_dict(orient="records")
    js_solicitacao = json.dumps(
        result_solicitacao, indent=4, sort_keys=True, default=str)
    dados_solicitacao = json.loads(js_solicitacao)

    @app_solicitacao.get('/')
    def inicio():
        return 'Banco de dados integrado com sucesso!'
except:
    @app_solicitacao.get('/')
    def inicio():
        return 'Falha ao integrar banco de dados.'

# Rota para inserir dados
@app_solicitacao.post('/solicitacoes')
def inserir_solicitacao(solicitacao: Solicitacao):
    dados_solicitacao.append(solicitacao)
    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password="8283", host="localhost")
    cur = conn.cursor()
    cur.execute(f"INSERT INTO solicitacao (taxId, activity, experience, state, city, value, installments, requestDate, source) VALUES\
	('{solicitacao.taxid}','{solicitacao.activity}','{solicitacao.experience}','{solicitacao.state}',\
        '{solicitacao.city}',{solicitacao.value},{solicitacao.installments},'{solicitacao.requestdate}',\
            '{solicitacao.source}');")
    conn.commit()
    return solicitacao

# Rota de dados
@app_solicitacao.get('/solicitacoes')
def get_solicitacoes():
    if dados_solicitacao == []:
        return 'Não há registro de solicitação'
    else:
        return dados_solicitacao

# Encerrar o banco de dados
if db_active:
    cur.close()
    conn.close()
