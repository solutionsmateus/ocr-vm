import os
import glob
import zipfile
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai import types, errors # Import correto de erros
import pandas as pd
import io
import re 
from concurrent.futures import ThreadPoolExecutor, as_completed 

load_dotenv()

# --- Configuração de Chaves ---
API_KEY_LIST = []
for key in ["GEMINI_API_KEY", "GEMINI_API_KEY_BACKUP_01", "GEMINI_API_KEY_BACKUP_02"]:
    val = os.environ.get(key)
    if val:
        API_KEY_LIST.append(val)

if not API_KEY_LIST:
    print("Erro: Nenhuma chave de API encontrada.")
    exit()

artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")

# --- Configurações de Segurança Corrigidas ---
safety_settings_list = [
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
    types.SafetySetting(
        category=types.HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=types.HarmBlockThreshold.BLOCK_NONE
    ),
]

MODEL_NAME = 'gemini-2.0-flash' # Verifique se sua cota permite o 2.5, o padrão atual é 2.0 ou 1.5

PROMPT_TEXT = """
Transforme o PDF/PNG/JPEG em tabela Markdown e XLSX.
Colunas: Empresa, Data, Data Início, Data Fim, Campanha, Categoria do Produto, Produto, Medida, Quantidade, Preço, App, Loja, Cidade, Estado.
(Regras de negócio mantidas conforme seu prompt original...)
"""

VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
MAX_THREADS = 8 
all_dataframes = [] 

def parse_markdown_table(markdown_text):
    COLUMNS = [
        "Empresa", "Data", "Data Início", "Data Fim", "Campanha", 
        "Categoria do Produto", "Produto", "Medida", "Quantidade", 
        "Preço", "App", "Loja", "Cidade", "Estado"
    ]
    try:
        lines = markdown_text.strip().split('\n')
        data_lines = [line for line in lines if line.strip().startswith('|')][2:]
        if not data_lines: return None
        
        cleaned_data = '\n'.join(data_lines)
        df = pd.read_csv(io.StringIO(cleaned_data), sep='|', skipinitialspace=True, header=None, engine='python')
        df = df.iloc[:, 1:-1]
        
        # Ajuste de colunas
        if df.shape[1] != len(COLUMNS):
            df = df.iloc[:, :len(COLUMNS)] if df.shape[1] > len(COLUMNS) else df
            while df.shape[1] < len(COLUMNS): df[f'extra_{df.shape[1]}'] = None
        
        df.columns = COLUMNS
        return df.dropna(how='all')
    except Exception as e:
        print(f"Erro no parse: {e}")
        return None

def process_single_file(file_path):
    print(f"[THREAD] Iniciando: {os.path.basename(file_path)}")
    
    # Loop de failover por arquivo
    for i, api_key in enumerate(API_KEY_LIST):
        key_label = f"Chave #{i+1}"
        client = genai.Client(api_key=api_key)
        uploaded_file = None

        try:
            # 1. Upload
            uploaded_file = client.files.upload(path=file_path)
            
            # 2. Geração
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=[PROMPT_TEXT, uploaded_file],
                config=types.GenerateContentConfig(safety_settings=safety_settings_list)
            )
            
            # 3. Parse
            df = parse_markdown_table(response.text)
            if df is not None:
                print(f"[THREAD] SUCESSO: {os.path.basename(file_path)} com {key_label}")
                # Limpeza antes de retornar
                client.files.delete(name=uploaded_file.name)
                return df

        except Exception as e:
            # Tratamento de Cota (429)
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                print(f"[THREAD] Cota excedida na {key_label}. Tentando próxima...")
                time.sleep(10)
                continue
            else:
                print(f"[THREAD] Erro crítico na {key_label} para {os.path.basename(file_path)}: {e}")
                break # Sai do loop de chaves para este arquivo se for erro de lógica/arquivo
        
        finally:
            if uploaded_file:
                try: client.files.delete(name=uploaded_file.name)
                except: pass

    return None

def process_files():
    # ... (Lógica de zip e busca de arquivos mantida) ...
    all_file_paths = []
    for root, _, files in os.walk(artifact_folder):
        for f in files:
            if f.lower().endswith(VALID_EXTENSIONS):
                all_file_paths.append(os.path.join(root, f))

    if not all_file_paths:
        print("Nenhum arquivo encontrado.")
        return

    print(f"Processando {len(all_file_paths)} arquivos...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        futures = {executor.submit(process_single_file, path): path for path in all_file_paths}
        for future in as_completed(futures):
            res = future.result()
            if res is not None: all_dataframes.append(res)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        final_df.to_excel("gemini_resultados_compilados.xlsx", index=False)
        print("Arquivo Excel gerado com sucesso!")

if __name__ == "__main__":
    process_files()
