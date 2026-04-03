import os
import json
import requests
from datetime import datetime

def extract_data(api_url):
    """Extrai os dados brutos da fonte de origem."""
    print(f"Extraindo dados da API: {api_url}")
    response = requests.get(api_url)
    response.raise_for_status() # Lança um erro se a requisição falhar
    return response.json()

def save_raw_data(data, base_path):
    """Salva os dados no formato bruto e cria partições por data."""
    # Particionamento comum em Data Lakes para facilitar a leitura futura
    now = datetime.now()
    partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    full_path = os.path.join(base_path, partition)
    
    # Cria os diretórios se não existirem
    os.makedirs(full_path, exist_ok=True)
    
    # Salva o arquivo em formato bruto (JSON) mantendo o timestamp
    file_name = f"raw_data_{now.strftime('%H%M%S')}.json"
    file_path = os.path.join(full_path, file_name)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    print(f"Dados salvos com sucesso em: {file_path}")

if __name__ == "__main__":
    print("Iniciando ingestão da camada Bronze...")
    
    # Configurações de exemplo do pipeline
    SOURCE_API = "https://jsonplaceholder.typicode.com/users"
    BRONZE_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "bronze", "users")
    
    try:
        raw_data = extract_data(SOURCE_API)
        save_raw_data(raw_data, BRONZE_DIR)
        print("Processo da camada Bronze finalizado com sucesso!")
    except Exception as e:
        print(f"Erro no pipeline da camada Bronze: {e}")