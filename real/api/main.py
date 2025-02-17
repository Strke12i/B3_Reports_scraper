from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from db import DatabaseConnection
from api import insert_data

app = FastAPI()

class FinancialData(BaseModel):
    company_name: str
    data: dict  # Dados financeiros processados

@app.post("/insert-data")
async def insert_financial_data(financial_data: FinancialData):
    try:
        db_connection = DatabaseConnection()
        session = db_connection.get_session()

        print("Inserindo dados no banco de dados...")
        print(financial_data.company_name)

        # Insere os dados no banco de dados
        insert_data(session, financial_data.data, financial_data.company_name)
        return {"message": "Dados inseridos com sucesso!"}
    except Exception as e:
        print(f"Erro ao inserir dados no banco de dados: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test")
async def test():
    return {"message": "Teste OK!"}