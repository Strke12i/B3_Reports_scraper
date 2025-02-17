from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import base64, os, cloudscraper, requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Constantes
STRUCTURED_REPORTS_URL = "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListStructuredReports/"
CVM_BASE_URL = "https://www.rad.cvm.gov.br/ENET/"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
COMPANIES_CSV_PATH = os.path.join(BASE_DIR, 'data', 'companies.csv')
scraper = cloudscraper.create_scraper()

# Funções para as tarefas
def get_company_info(company_name):
    df_companies = pd.read_csv(COMPANIES_CSV_PATH)
    company_info = df_companies[df_companies["issuingCompany"] == company_name]
    if company_info.empty:
        raise ValueError(f"Empresa '{company_name}' não encontrada.")
    return company_info.iloc[0]['codeCVM']

def get_structured_reports(code_cvm, year):
    payload = f'{{"codeCVM":{code_cvm},"language":"pt-br","status":true,"year":{year}}}'
    encoded_payload = base64.b64encode(payload.encode()).decode()
    url = STRUCTURED_REPORTS_URL + encoded_payload
    response = scraper.get(url)
    if response.status_code != 200:
        raise Exception(f"Erro na requisição. Status code: {response.status_code}")
    return response.json()

def extract_url_search(data_json):
    try:
        return data_json["dfp"][0]["urlSearch"]
    except Exception as e:
        print(f"Erro ao extrair URL: {e}")
        return None

def extract_links_from_select(url_report):
    try:
        response = scraper.get(url_report)
        soup = BeautifulSoup(response.content, "html.parser")
        select = soup.find("select", {"id": "cmbQuadro"})
        options = select.find_all("option")

        script = soup.find_all("script")[-1]
        link_id = script.string.split("location=")[1].split("'")[1].split("Versao=")[1]

        links = {
            option.text: f"{CVM_BASE_URL}{option['value'].replace(' ', '%20')}{link_id}"
            for option in options
        }
        
        responses = {}
        for title, link in links.items():
            try:
                response = scraper.get(link)
                responses[title] = base64.b64encode(response.content).decode('utf-8')
            except Exception as e:
                print(f"Erro ao extrair link: {e}")
                responses[title] = None
        
        return responses
    except Exception as e:
        print(f"Erro ao extrair links: {e}")
        return None

def get_table_data(response):
    decoded_content = base64.b64decode(response).decode('utf-8')
    soup = BeautifulSoup(decoded_content, "html.parser")
    table = soup.find("table", {"id": "ctl00_cphPopUp_tbDados"})

    if not table:
        raise Exception("Tabela não encontrada no link fornecido.")

    all_tr = table.find_all("tr")
    data = [[td.text.strip() for td in tr.find_all("td")] for tr in all_tr]
    
    print(data)

    return pd.DataFrame(data[1:], columns=data[0])

def reorganize_df(df):
    df.index = df['Descrição']
    df = df.drop(columns=['Descrição', 'Conta'])
    
    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = df[column].str.replace('.', '')
            df[column] = df[column].str.replace(',', '.')
            df[column] = pd.to_numeric(df[column], errors='coerce')
            df[column] = df[column].fillna(0)
    
    if df.index.duplicated().any():
        df.index = df.index + df.groupby(level=0).cumcount().astype(str).replace('0', '')
    
    return df

def fetch_data_from_data_type(url_report, links, company_name, data_type, year):
    
    if not url_report:
        raise ValueError(f"Não foi possível localizar relatórios para '{company_name}' no ano {year}.")
    
    if not links or data_type not in links:
        raise ValueError(f"Tipo de dado '{data_type}' não disponível para '{company_name}'.")

    response = links[data_type]
    df = get_table_data(response)
    df = reorganize_df(df)
    
    return df

def fetch_all_data(url_report, links, company_name, year):
    data_types = [
            "Balanço Patrimonial Ativo",
            "Balanço Patrimonial Passivo",
            "Demonstração do Resultado",
            "Demonstração do Resultado Abrangente",
            "Demonstração do Fluxo de Caixa",
            "Demonstração de Valor Adicionado",
    ]
    data = {}
    for data_type in data_types:
        try:
            df = fetch_data_from_data_type(url_report, links, company_name, data_type, year)
            data[data_type] = df
        except Exception as e:
            print(f"Erro ao extrair dados de '{data_type}': {e}")
            data[data_type] = None
    return data

def get_all_itrs_links(data_json):
        itr_links = {}
        try:
            itr = data_json['itr']
            for i in itr:
                date = i['dateTimeReference'].split('T')[0]
                itr_links[date] = i['urlSearch']
                
            return itr_links
        except Exception as e:
            print("Erro ao extrair links de ITR.")
            return itr_links

def transform_data(data: dict, year_rec) -> dict:
    for key in data.keys():
        if key in ['Balanço Patrimonial Ativo', 'Balanço Patrimonial Passivo']:
            for column in data[key].columns:
                year = column.split('/')[-1]
                if year != year_rec:
                    new_column = f'01/01/{year}  a  31/12/{year}'
                    data[key].rename(columns={column: new_column}, inplace=True)
                else:
                    new_column = f'01/01/{year}  a  {column}'
                    data[key].rename(columns={column: new_column}, inplace=True)

    return data

def fetch_itr_data(url_report, dfp_links ,itr_links, company_name, year):
    if not itr_links:
        raise ValueError("Não foi possível localizar relatórios ITR.")
    
    itr_data = {}
    
    for date, link in itr_links.items():
        itr_data[date] = fetch_all_data(link, extract_links_from_select(link), company_name, year)
    
    end_year = fetch_all_data(url_report, dfp_links, company_name, year + 1)
    formated_date = f'{year}-12-31'
    
    itr_data[formated_date] = end_year
    for key in itr_data.keys():
        itr_data[key] = transform_data(itr_data[key], year)
        
    return itr_data

def convert_df_to_dict(data: dict) -> dict:
    for key in data.keys():
        if data[key] is not None:
            for k in data[key].keys():
                if data[key][k] is not None:
                    data[key][k] = data[key][k].to_dict()
    return data

def send_data_to_api(data, company_name):
    data = convert_df_to_dict(data)
    dates = sorted(data.keys())
    for date in dates:
        data_to_send = data[date]
        print(f"Data {date} Enviando dados de {company_name} para a API: {data_to_send}")
        # Aqui você faria a requisição para a API
        api_url = 'http://api:8000/insert-data'
        req = requests.post(api_url, json={"company_name": company_name, "data": data_to_send})
        if req.status_code != 200:
            print(f"Erro ao enviar dados para a API. Status code: {req.status_code}")
            raise Exception("Erro ao enviar dados para a API.")
    
    print("Dados enviados com sucesso!")

# Se conectar ao banco de dados para dar refresh na view materializada Indicadores
# O banco está rodando em um container chamado 'db'
def refresh_indicators_view():
    engine = create_engine('postgresql+psycopg2://admin:admin_password@db:5432/meu_banco')
    Session = sessionmaker(bind=engine)
    session = Session()
    session.execute('REFRESH MATERIALIZED VIEW Indicadores')
    session.commit()
    session.close()

# Definindo a DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'retries': 1,
}

dag = DAG(
    'b3_reports_pipeline',
    default_args=default_args,
    description='Pipeline para buscar e processar relatórios da B3',
    schedule_interval='@daily',
)

empresa = 'MGLU'
ano = 2023
t1 = PythonOperator(
    task_id='get_company_info',
    python_callable=get_company_info,
    op_args=[empresa],  # Nome da empresa
    dag=dag,
)

t2 = PythonOperator(
    task_id='get_structured_reports',
    python_callable=get_structured_reports,
    op_args=[t1.output, ano],  # Ano do relatório
    dag=dag,
)

t21_ = PythonOperator(
    task_id='get_structured_reports_next_year',
    python_callable=get_structured_reports,
    op_args=[t1.output, ano + 1],  # Ano do relatório
    dag=dag,
)

t3 = PythonOperator(
    task_id='url_report',
    python_callable=extract_url_search,
    op_args=[t21_.output],
    dag=dag,
)

t4 = PythonOperator(
    task_id='dfp_links',
    python_callable=extract_links_from_select,
    op_args=[t3.output],
    dag=dag,
)

t5 = PythonOperator(
    task_id='itr_links',
    python_callable=get_all_itrs_links,
    op_args=[t2.output],
    dag=dag,
)

t6 = PythonOperator(
    task_id='fetch_itr_data',
    python_callable=fetch_itr_data,
    op_args=[t3.output, t4.output, t5.output, empresa, ano],
    dag=dag,
)

t7 = PythonOperator(
    task_id='send_data_to_api',
    python_callable=send_data_to_api,
    op_args=[t6.output, empresa],
    dag=dag,
)

t8 = PythonOperator(
    task_id='refresh_indicators_view',
    python_callable=refresh_indicators_view,
    dag=dag,
)


# Definindo a ordem das tarefas
t1 >> t2 >> t21_ >> t3 >> t4 >> t5 >> t6 >> t7 >> t8