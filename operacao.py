from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2
from datetime import date
import json
import pandas.io.sql as sqlio

# Criar API
app_operacao = FastAPI()

dados_operacao = []

# Criar model operação
class Operacao(BaseModel):
    taxid: str
    valueasked: float
    value: float
    installments: int
    interestrate: float
    approvaldate: date
    releasedate: date
    state: str
    city: str


# Verificar status do banco de dados
db_active = False
try:
    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password="8283", host="localhost")
    cur = conn.cursor()

    # Lendo o arquivo SQL e convertendo-o de DataFrame para JSON
    bd_operacao = sqlio.read_sql_query("SELECT * FROM operacao;", conn)
    result_operacao = bd_operacao.to_dict(orient='records')
    js_operacao = json.dumps(result_operacao, indent=4,
                             sort_keys=True, default=str)
    dados_operacao = json.loads(js_operacao)

    @app_operacao.get('/')
    def inicio():
        return 'Banco de dados integrado com sucesso!'
except:
    @app_operacao.get('/')
    def inicio():
        return 'Falha ao integrar banco de dados.'

# Inserir dados
@app_operacao.post('/operacoes')
def inserir_operacao(operacao: Operacao):
    dados_operacao.append(operacao)
    conn = psycopg2.connect(dbname="postgres", user="postgres",
                            password="8283", host="localhost")
    cur = conn.cursor()
    cur.execute(f"INSERT INTO operacao (taxId, valueAsked, value, installments, interestRate, approvalDate, releaseDate, state, city) VALUES\
	('{operacao.taxid}',{operacao.valueAsked},{operacao.value},{operacao.installments},\
        {operacao.interestrate},'{operacao.approvaldate}','{operacao.releasedate}','{operacao.state}',\
            '{operacao.city}');")
    conn.commit()
    return operacao

# Rota de dados
@app_operacao.get('/operacoes')
def get_operacao():
    if dados_operacao == []:
        return 'Não há registro de operação'
    else:
        return dados_operacao


# Encerrar o banco de dados
if db_active:
    cur.close()
    conn.close()
