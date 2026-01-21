import os
import glob
import zipfile
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai.errors import APIError 
from google.genai.types import HarmCategory, HarmBlockThreshold, GenerateContentConfig, SafetySetting 
import pandas as pd
import io
import re 
from concurrent.futures import ThreadPoolExecutor, as_completed 

load_dotenv()

API_KEY_LIST = []
for env_var in ["GEMINI_API_KEY", "GEMINI_API_KEY_BACKUP_01", "GEMINI_API_KEY_BACKUP_02"]:
    key = os.environ.get(env_var)
    if key:
        API_KEY_LIST.append(key)

if not API_KEY_LIST:
    print("Erro: Nenhuma chave de API encontrada.")
    exit()

artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")

safety_settings_list = [
    SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
]

MODEL_NAME = 'gemini-2.0-flash' # Ajustado para versão estável atual 

PROMPT_TEXT = """
Transforme o arquivo em uma tabela Markdown. 
Use EXATAMENTE estas colunas: Empresa, Data, Data Início, Data Fim, Campanha, Categoria do Produto, Produto, Medida, Quantidade, Preço, App, Loja, Cidade, Estado.
Não use o caractere pipe (|) dentro das células.
"""

VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
MAX_THREADS = 4 # Reduzido para evitar 429 excessivo

all_dataframes = [] 

def parse_markdown_table(markdown_text):
    COLUMNS = [
        "Empresa", "Data", "Data Início", "Data Fim", "Campanha", 
        "Categoria do Produto", "Produto", "Medida", "Quantidade", 
        "Preço", "App", "Loja", "Cidade", "Estado"
    ]
    try:
        # Limpeza para remover blocos de código markdown se existirem
        text = re.sub(r'```markdown|```', '', markdown_text).strip()
        lines = text.split('\n')
        
        # Filtra apenas linhas que parecem tabelas
        data_lines = [line.strip() for line in lines if line.strip().startswith('|')]
        
        if len(data_lines) < 2: return None

        # Remove a linha de separação (|---|---|)
        content_lines = [data_lines[0]] + [l for l in data_lines[1:] if not re.match(r'^[|:\-\s]+$', l)]
        
        cleaned_data = '\n'.join(content_lines)
        df = pd.read_csv(io.StringIO(cleaned_data), sep='|', skipinitialspace=True, engine='python')
        
        # Remove colunas vazias geradas pelos pipes nas extremidades
        df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
        df.columns = [c.strip() for c in df.columns]

        # Garantir que todas as colunas existam
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = None
        
        return df[COLUMNS]
    except Exception as e:
        print(f"Erro no parse: {e}")
        return None

def process_single_file(file_path):
    filename = os.path.basename(file_path)
    print(f"[THREAD] Iniciando: {filename}")
    
    # Tentativa de Failover englobando Upload + Geração para evitar erro 403
    for i, api_key in enumerate(API_KEY_LIST):
        client = genai.Client(api_key=api_key)
        uploaded_file = None
        
        try:
            # Upload vinculado à chave atual
            uploaded_file = client.files.upload(file=Path(file_path))
            
            # Aguarda brevemente para processamento do arquivo no servidor
            time.sleep(1) 

            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=[PROMPT_TEXT, uploaded_file],
                config=GenerateContentConfig(safety_settings=safety_settings_list)
            )
            
            df = parse_markdown_table(response.text)
            
            # Limpeza
            if uploaded_file:
                client.files.delete(name=uploaded_file.name)
                
            if df is not None:
                print(f"[THREAD] SUCESSO: {filename}")
                return df
            else:
                return None

        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                print(f"[THREAD] Cota esgotada na chave {i+1} para {filename}. Tentando próxima...")
                if uploaded_file:
                    try: client.files.delete(name=uploaded_file.name)
                    except: pass
                continue
            else:
                print(f"[THREAD] ERRO FATAL {filename} na chave {i+1}: {e}")
                if uploaded_file:
                    try: client.files.delete(name=uploaded_file.name)
                    except: pass
                break # Erros 400, 403 permanentes não resolvem com troca de chave
                
    return None

def process_files():
    # ... (Lógica de busca de arquivos simplificada para o exemplo)
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
            if res is not None:
                all_dataframes.append(res)

    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        final_df.to_excel("gemini_resultados_compilados.xlsx", index=False)
        print("Arquivo salvo com sucesso!")

if __name__ == "__main__":
    process_files()
