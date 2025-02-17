import base64
import pandas as pd
import cloudscraper
from bs4 import BeautifulSoup


class B3ReportsFetcher:
    """
    Classe para buscar relatórios estruturados e dados financeiros de empresas listadas na B3.
    """

    STRUCTURED_REPORTS_URL = (
        "https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListStructuredReports/"
    )
    CVM_BASE_URL = "https://www.rad.cvm.gov.br/ENET/"

    def __init__(self):
        """
        Inicializa o scraper.
        """
        self.scraper = cloudscraper.create_scraper()
        self.df_companies = pd.read_csv('companies.csv')

    def get_structure_reports(self, code_cvm, year) -> dict:
        """
        Busca relatórios estruturados para uma empresa específica com base no código CVM e ano.
        """
        payload = f'{{"codeCVM":{code_cvm},"language":"pt-br","status":true,"year":{year}}}'
        encoded_payload = base64.b64encode(payload.encode()).decode()
        url = self.STRUCTURED_REPORTS_URL + encoded_payload
        response = self.scraper.get(url)

        if response.status_code != 200:
            raise Exception(f"Erro na requisição. Status code: {response.status_code}")

        return response

    @staticmethod
    def extract_url_search(data) -> str:
        """
        Extrai a URL de busca do relatório estruturado.
        """
        try:
            data_json = data.json()
            return data_json["dfp"][0]["urlSearch"]
        except Exception as e:
            print(f"Erro ao extrair URL: {e}")
            return None

    @staticmethod
    def extract_links_from_select(response) -> dict:
        """
        Extrai os links de relatórios disponíveis de um select HTML.
        """
        try:
            soup = BeautifulSoup(response.content, "html.parser")
            select = soup.find("select", {"id": "cmbQuadro"})
            options = select.find_all("option")

            script = soup.find_all("script")[-1]
            link_id = script.string.split("location=")[1].split("'")[1].split("Versao=")[1]

            links = {
                option.text: f"{B3ReportsFetcher.CVM_BASE_URL}{option['value'].replace(' ', '%20')}{link_id}"
                for option in options
            }
            return links
        except Exception as e:
            print(f"Erro ao extrair links: {e}")
            return None

    def get_table_data(self, link: str) -> pd.DataFrame:
        """
        Busca e retorna os dados de uma tabela específica do relatório.
        """
        response = self.scraper.get(link)
        soup = BeautifulSoup(response.content, "html.parser")
        table = soup.find("table", {"id": "ctl00_cphPopUp_tbDados"})

        if not table:
            raise Exception("Tabela não encontrada no link fornecido.")

        all_tr = table.find_all("tr")
        data = [[td.text.strip() for td in tr.find_all("td")] for tr in all_tr]

        # Criação do DataFrame com as colunas da primeira linha
        return pd.DataFrame(data[1:], columns=data[0])

    def fetch_financial_data(self, company_name: str, data_type: str, year: int) -> pd.DataFrame:
        """
        Busca e retorna os dados financeiros de uma empresa específica na B3.

        Args:
            company_name (str): Nome da empresa (issuingCompany).
            data_type (str): Tipo de dado desejado. Exemplo:
                            - 'Balanço Patrimonial Ativo'
                            - 'Balanço Patrimonial Passivo'
                            - 'Demonstração do Resultado'
                            - 'Demonstração do Resultado Abrangente'
                            - 'Demonstração do Fluxo de Caixa'
                            - 'Demonstração das Mutações do Patrimônio Líquido'
                            - 'Demonstração de Valor Adicionado'
            year (int): Ano do relatório.

        Returns:
            pd.DataFrame: DataFrame contendo os dados solicitados.

        Raises:
            ValueError: Caso a empresa ou o tipo de dado não seja encontrado.
            Exception: Caso algum erro ocorra durante a requisição ou parsing.
        """
        

        # Buscar informações das empresas

        # Verificar se a empresa existe
        test_data = self.df_companies[self.df_companies["issuingCompany"] == company_name]
        if test_data.empty:
            raise ValueError(f"Empresa '{company_name}' não encontrada.")

        # Obter código CVM
        code_cvm = test_data["codeCVM"].values[0]

        # Buscar relatórios estruturados
        response = self.get_structure_reports(code_cvm, year)
        url_report = self.extract_url_search(response)

        if not url_report:
            raise ValueError(f"Não foi possível localizar relatórios para '{company_name}' no ano {year}.")

        # Obter links de relatórios
        url_response = self.scraper.get(url_report)
        links = self.extract_links_from_select(url_response)

        if not links or data_type not in links:
            raise ValueError(f"Tipo de dado '{data_type}' não disponível para '{company_name}'.")

        # Obter os dados da tabela desejada
        link = links[data_type]
        df = self.get_table_data(link)

        return df
    
    def get_all_financial_data(self, company_name: str, year: int) -> dict:
        """
        Busca e retorna todos os dados financeiros de uma empresa específica na B3.

        Args:
            company_name (str): Nome da empresa (issuingCompany).
            year (int): Ano do relatório.

        Returns:
            dict: Dicionário contendo os DataFrames com os dados solicitados.

        Raises:
            ValueError: Caso a empresa não seja encontrada.
            Exception: Caso algum erro ocorra durante a requisição ou parsing.
        """
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
                df = self.fetch_financial_data(company_name, data_type, year)
                data[data_type] = df
            except Exception as e:
                print(f"Erro ao buscar dados de '{data_type}': {e}")

        return data

    @staticmethod
    def get_all_itrs_links(data):
        itr_links = {}
        try:
            data_json = data.json()
            itr = data_json['itr']
            print(itr)
            for i in itr:
                date = i['dateTimeReference'].split('T')[0]
                itr_links[date] = i['urlSearch']
            
            return itr_links
        except Exception as e:
            print(e)
            print("Erro ao extrair links de ITR.")
            return itr_links
        
    
    def fetch_finacial_data_with_link(self, url_report: str, company_name: str, data_type: str) -> pd.DataFrame:
        if not url_report:
            raise ValueError(f"Não foi possível localizar relatórios para '{company_name}' no ano {year}.")

        # Obter links de relatórios
        url_response = self.scraper.get(url_report)
        links = self.extract_links_from_select(url_response)

        if not links or data_type not in links:
            raise ValueError(f"Tipo de dado '{data_type}' não disponível para '{company_name}'.")

        # Obter os dados da tabela desejada
        link = links[data_type]
        df = self.get_table_data(link)

        return df
    
    def get_all_itrl_trim(self, company_name, link) -> dict:
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
                df = self.fetch_finacial_data_with_link(link, company_name, data_type)
                data[data_type] = df
            except Exception as e:
                print(f"Erro ao buscar dados de '{data_type}': {e}")

        return data
    
    def reorganize_df(self, df: pd.DataFrame) -> pd.DataFrame:
        df.index = df['Descrição']
        df = df.drop(columns=['Descrição', 'Conta'])
        
        for column in df.columns:
            if df[column].dtype == 'object':
                df[column] = df[column].str.replace('.', '')
                df[column] = df[column].str.replace(',', '.')
                
                df[column] = df[column].apply(pd.to_numeric, errors='coerce')
                df[column] = df[column].fillna(0)
                
        # Verificar se existem index duplicados, caso haja, adicionar '{i}' ao final do index dos duplicados
        if df.index.duplicated().any():
            df.index = df.index + df.groupby(level=0).cumcount().astype(str).replace('0', '')
            
        return df
    
    
    def transform_data(self, data: dict, year_rec) -> dict:
        for key in data.keys():
            data[key] = self.reorganize_df(data[key])
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
    
    
    
    def get_itr_data(self, company_name, year) -> dict:
        """
        Busca relatórios ITR para uma empresa específica com base no código CVM e ano.
        """
        # Buscar informações das empresas
        code_cvm = self.df_companies[self.df_companies["issuingCompany"] == company_name]["codeCVM"].values[0]
        response = self.get_structure_reports(code_cvm, year)
        links = self.get_all_itrs_links(response)
        data = {}
        
        for date, url in links.items():
            data[date] = self.get_all_itrl_trim(code_cvm, url)
            
        end_year = self.get_all_financial_data(company_name, year + 1)
        formated_date = f'{year}-12-31'
        
        data[formated_date] = end_year
        
        for key in data.keys():
            data[key] = self.transform_data(data[key], str(year))
            
        
        
        return data


if __name__ == "__main__":
    company = "WEGE"  # Nome da empresa (issuingCompany)
    data_type = "Demonstração do Resultado"  # Tipo de dado desejado
    year = 2023  # Ano do relatório
    
    from db import db, api
    database_connection = db.DatabaseConnection()
    session = database_connection.get_session()
    
    b3ReportsFetcher = B3ReportsFetcher()
    rensponse = b3ReportsFetcher.get_itr_data(company, year)
    
    datas = rensponse.keys()
    datas = sorted(datas)
    for key in datas:
        api.insert_data(session, rensponse[key], company)
