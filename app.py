import os
import glob
import zipfile
import time
import io
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai.types import HarmCategory, HarmBlockThreshold, GenerateContentConfig, SafetySetting
import pandas as pd

# --- 1. Configuração Inicial ---
load_dotenv()

# Lista de chaves para Failover
API_KEYS = []
for k in ["GEMINI_API_KEY", "GEMINI_API_KEY_BACKUP_01", "GEMINI_API_KEY_BACKUP_02"]:
    val = os.environ.get(k)
    if val:
        API_KEYS.append(val)

artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")

if not API_KEYS:
    print("ERRO: Nenhuma chave API encontrada.")
    exit()

# Modelo estável para evitar flutuação de cota
MODEL_NAME = 'gemini-2.5-flash' 

# Configuração de Segurança
safety_settings_list = [
    SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
]

PROMPT_TEXT = """
Transforme o PDF/PNG/JPEG em tabela Markdown e XLSX.
Colunas: Empresa, Data, Data Início, Data Fim, Campanha, Categoria do Produto, Produto, Medida, Quantidade, Preço, App, Loja, Cidade, Estado.
[Regras de negócio originais...]
"""

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
        
        df = pd.read_csv(io.StringIO('\n'.join(data_lines)), sep='|', skipinitialspace=True, header=None, engine='python')
        df = df.iloc[:, 1:-1]
        
        if df.shape[1] != len(COLUMNS):
            if df.shape[1] > len(COLUMNS): df = df.iloc[:, :len(COLUMNS)]
            else:
                for i in range(len(COLUMNS) - df.shape[1]): df[f'extra_{i}'] = None
        
        df.columns = COLUMNS
        return df.dropna(how='all')
    except:
        return None

def process_files():
    # 1. Extração de Zips
    zip_files = glob.glob(os.path.join(artifact_folder, "**", "*.zip"), recursive=True)
    for zp in zip_files:
        try:
            with zipfile.ZipFile(zp, 'r') as zr:
                zr.extractall(os.path.dirname(zp))
        except:
            continue

    # 2. Coleta de arquivos
    VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
    all_paths = []
    for root, _, files in os.walk(artifact_folder):
        for f in files:
            if f.lower().endswith(VALID_EXTENSIONS):
                all_paths.append(os.path.join(root, f))

    if not all_paths:
        print("Nenhum arquivo encontrado para processar.")
        return

    print(f"Iniciando processamento de {len(all_paths)} arquivos (Limite: 5 RPM)...")
    
    all_dataframes = []
    current_key_idx = 0
    
    for idx, path in enumerate(all_paths):
        if current_key_idx >= len(API_KEYS):
            print("FALHA: Todas as chaves API esgotaram.")
            break
            
        success = False
        file_name = os.path.basename(path)
        print(f"[{idx+1}/{len(all_paths)}] Processando: {file_name}")

        while not success and current_key_idx < len(API_KEYS):
            client = genai.Client(api_key=API_KEYS[current_key_idx])
            uploaded_file = None
            
            try:
                # Upload do arquivo
                uploaded_file = client.files.upload(file=path)
                
                # Pausa estratégica para não atropelar o limite de 5 RPM
                # Ciclo total desejado: 12-15 segundos por arquivo
                time.sleep(5) 

                response = client.models.generate_content(
                    model=MODEL_NAME,
                    contents=[uploaded_file, PROMPT_TEXT],
                    config=GenerateContentConfig(safety_settings=safety_settings_list)
                )
                
                df = parse_markdown_table(response.text)
                if df is not None:
                    all_dataframes.append(df)
                    print(f"  OK: Processado com Chave {current_key_idx + 1}")
                
                success = True # Arquivo finalizado com sucesso

            except Exception as e:
                err_msg = str(e).upper()
                if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                    print(f"  AVISO: Cota da Chave {current_key_idx + 1} excedida. Tentando proxima...")
                    current_key_idx += 1
                    time.sleep(5)
                else:
                    print(f"  ERRO no arquivo {file_name}: {e}")
                    success = True # Pula o arquivo em caso de erro de conteúdo
            
            finally:
                if uploaded_file:
                    try: client.files.delete(name=uploaded_file.name)
                    except: pass
                # Pausa final do ciclo para garantir o RPM
                time.sleep(7)

    # Salva o resultado final
    if all_dataframes:
        final_df = pd.concat(all_dataframes, ignore_index=True)
        output_name = "gemini_resultados_compilados.xlsx"
        final_df.to_excel(output_name, index=False)
        print(f"SUCESSO: Resultado salvo em {output_name}")
    else:
        print("FIM: Nenhum dado foi extraido.")

if __name__ == "__main__":
    process_files()
