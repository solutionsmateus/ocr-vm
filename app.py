import os
import glob
import zipfile
import time
import io
import re
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai import types
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# --- Configuração de Chaves e Variáveis de Ambiente ---
API_KEY_LIST = []
for key in ["GEMINI_API_KEY", "GEMINI_API_KEY_BACKUP_01", "GEMINI_API_KEY_BACKUP_02"]:
    val = os.environ.get(key)
    if val:
        API_KEY_LIST.append(val)

if not API_KEY_LIST:
    print("Erro: Nenhuma chave de API encontrada nas variáveis de ambiente.")
    exit()

artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")
MODEL_NAME = 'gemini-2.0-flash'  # Versão estável e rápida
MAX_THREADS = 8 
VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')

# --- Configurações de Segurança ---
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

PROMPT_TEXT = """
Transforme o PDF/PNG/JPEG em tabela Markdown (para copiar no Excel) e XLSX, usando esta ordem EXATA de colunas:
Empresa, Data, Data Início, Data Fim, Campanha, Categoria do Produto, Produto, Medida, Quantidade, Preço, App, Loja, Cidade, Estado.

✅ REGRAS OBRIGATÓRIAS:
- Empresa: Apenas Assaí Atacadista, Atacadão, Cometa Supermercados, Frangolândia, GBarbosa, Atakarejo, Novo Atakarejo.
- Loja: Cidades onde o encarte atua (separadas por ;).
- Cidade/Estado: Conforme tabela de capitais fornecida.
- Proibido usar o caractere pipe (|) dentro dos campos.
- Caso Cometa: Cidade/Loja = Fortaleza, Estado = CEARÁ.
- Caso Novo Atakarejo: Loja = Olinda, Cidade = Recife, Estado = PERNAMBUCO.
"""

def parse_markdown_table(markdown_text):
    COLUMNS = [
        "Empresa", "Data", "Data Início", "Data Fim", "Campanha", 
        "Categoria do Produto", "Produto", "Medida", "Quantidade", 
        "Preço", "App", "Loja", "Cidade", "Estado"
    ]
    try:
        lines = markdown_text.strip().split('\n')
        data_lines = [line for line in lines if line.strip().startswith('|')]
        if len(data_lines) < 3: return None
        
        cleaned_data = '\n'.join(data_lines[2:])
        df = pd.read_csv(io.StringIO(cleaned_data), sep='|', skipinitialspace=True, header=None, engine='python')
        df = df.iloc[:, 1:-1] # Remove bordas vazias
        
        if df.shape[1] != len(COLUMNS):
            if df.shape[1] > len(COLUMNS):
                df = df.iloc[:, :len(COLUMNS)]
            else:
                while df.shape[1] < len(COLUMNS):
                    df[f'extra_{df.shape[1]}'] = None
        
        df.columns = COLUMNS
        return df.dropna(how='all')
    except Exception as e:
        print(f"Erro no parse: {e}")
        return None

def process_single_file(file_path):
    file_name = os.path.basename(file_path)
    
    for i, api_key in enumerate(API_KEY_LIST):
        key_label = f"Chave #{i+1}"
        client = genai.Client(api_key=api_key)
        uploaded_file = None

        try:
            print(f"[THREAD] Tentando {file_name} com {key_label}...")
            # 'file' é o argumento correto para o caminho na SDK google-genai
            uploaded_file = client.files.upload(file=file_path)
            
            # Aguarda breve processamento do arquivo no servidor
            time.sleep(1)

            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=[uploaded_file, PROMPT_TEXT],
                config=types.GenerateContentConfig(safety_settings=safety_settings_list)
            )
            
            df = parse_markdown_table(response.text)
            if df is not None:
                print(f"[THREAD] SUCESSO: {file_name}")
                return df

        except Exception as e:
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                print(f"[THREAD] Limite atingido na {key_label}. Alternando...")
                time.sleep(5)
                continue
            else:
                print(f"[THREAD] Erro em {file_name} ({key_label}): {e}")
                continue
        finally:
            if uploaded_file:
                try: client.files.delete(name=uploaded_file.name)
                except: pass

    print(f"[THREAD] FALHA TOTAL: {file_name}")
    return None

def main():
    # Extração de Zips
    zip_files = glob.glob(os.path.join(artifact_folder, "**", "*.zip"), recursive=True)
    for zip_path in zip_files:
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(os.path.dirname(zip_path))
        except Exception as e:
            print(f"Erro no zip {zip_path}: {e}")

    # Coleta de Arquivos
    all_paths = []
    for root, _, files in os.walk(artifact_folder):
        for f in files:
            if f.lower().endswith(VALID_EXTENSIONS):
                all_paths.append(os.path.join(root, f))

    if not all_paths:
        print("Nenhum arquivo para processar.")
        return

    print(f"Iniciando processamento de {len(all_paths)} arquivos...")
    results = []
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        future_to_file = {executor.submit(process_single_file, p): p for p in all_paths}
        for future in as_completed(future_to_file):
            res = future.result()
            if res is not None:
                results.append(res)

    if results:
        final_df = pd.concat(results, ignore_index=True)
        output = "gemini_resultados_compilados.xlsx"
        final_df.to_excel(output, index=False)
        print(f"\nConcluído! Resultado salvo em: {output}")
    else:
        print("\nNenhum dado extraído.")

if __name__ == "__main__":
    main()
