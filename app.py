import os
import glob
import zipfile
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
from google.genai.types import HarmCategory, HarmBlockThreshold, GenerateContentConfig, SafetySetting
import pandas as pd
import io

load_dotenv()

API_KEYS = [
    os.environ.get("GEMINI_API_KEY"),
    os.environ.get("GEMINI_API_KEY_BACKUP_01"),
    os.environ.get("GEMINI_API_KEY_BACKUP_02")
]
API_KEYS = [k for k in API_KEYS if k] # Remove None

artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")
MODEL_NAME = 'gemini-2.5-flash' 

safety_settings_list = [
    SafetySetting(category=HarmCategory.HARM_CATEGORY_HARASSMENT, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_HATE_SPEECH, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT, threshold=HarmBlockThreshold.BLOCK_NONE),
    SafetySetting(category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT, threshold=HarmBlockThreshold.BLOCK_NONE),
]

PROMPT_TEXT = """[SEU PROMPT ORIGINAL AQUI]"""

def parse_markdown_table(markdown_text):
    # [SUA FUNÇÃO DE PARSE ORIGINAL AQUI - ELA ESTÁ CORRETA]
    COLUMNS = ["Empresa", "Data", "Data Início", "Data Fim", "Campanha", "Categoria do Produto", "Produto", "Medida", "Quantidade", "Preço", "App", "Loja", "Cidade", "Estado"]
    try:
        lines = markdown_text.strip().split('\n')
        data_lines = [line for line in lines if line.strip().startswith('|')][2:]
        if not data_lines: return None
        df = pd.read_csv(io.StringIO('\n'.join(data_lines)), sep='|', skipinitialspace=True, header=None, engine='python')
        df = df.iloc[:, 1:-1]
        if df.shape[1] > len(COLUMNS): df = df.iloc[:, :len(COLUMNS)]
        elif df.shape[1] < len(COLUMNS):
            for i in range(len(COLUMNS) - df.shape[1]): df[f'col_{i}'] = None
        df.columns = COLUMNS
        return df.dropna(how='all')
    except: return None

def process_files():
    # Extração de Zips (Igual ao seu)
    zip_files = glob.glob(os.path.join(artifact_folder, "**", "*.zip"), recursive=True)
    for zp in zip_files:
        try:
            with zipfile.ZipFile(zp, 'r') as zr: zr.extractall(os.path.dirname(zp))
        except: pass

    # Coleta de todos os caminhos válidos
    VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
    file_paths = []
    for root, _, files in os.walk(artifact_folder):
        for f in files:
            if f.lower().endswith(VALID_EXTENSIONS):
                file_paths.append(os.path.join(root, f))

    if not file_paths:
        print("Nenhum arquivo encontrado.")
        return

    print(f"Total de arquivos: {len(file_paths)}. Limite: 5 RPM. Iniciando...")
    
    all_dfs = []
    current_key_index = 0
    
    for idx, path in enumerate(file_paths):
        success = False
        print(f"\n[{idx+1}/{len(file_paths)}] Processando: {os.path.basename(path)}")
        
        # Tentativa de processar o arquivo com Failover de Chaves
        while not success and current_key_index < len(API_KEYS):
            client = genai.Client(api_key=API_KEYS[current_key_index])
            uploaded_file = None
            
            try:
                # Upload
                uploaded_file = client.files.upload(file=Path(path))
                
                # Backoff preventivo: Com 5 RPM, cada operação deve levar ~12 segundos
                # O upload + geração geralmente leva uns 8s. Vamos esperar 4s extras.
                time.sleep(4) 

                response = client.models.generate_content(
                    model=MODEL_NAME,
                    contents=[uploaded_file, PROMPT_TEXT],
                    config=GenerateContentConfig(safety_settings=safety_settings_list)
                )
                
                df = parse_markdown_table(response.text)
                if df is not None:
                    all_dfs.append(df)
                    print(f"✅ Sucesso com Chave {current_key_index + 1}")
                
                success = True # Sai do loop do arquivo

            except Exception as e:
                err = str(e).upper()
                if "429" in err or "EXHAUSTED" in err:
                    print(f"⚠️ Cota da Chave {current_key_index + 1} atingida. Trocando...")
                    current_key_index += 1
                    time.sleep(2)
                else:
                    print(f"❌ Erro fatal no arquivo: {e}")
                    success = True # Pula o arquivo para não travar o script
            
            finally:
                if uploaded_file:
                    try: client.files.delete(name=uploaded_file.name)
                    except: pass
                # Pausa obrigatória para respeitar os 5 RPM (60s / 5 = 12s por ciclo)
                time.sleep(8) 

    if all_dfs:
        pd.concat(all_dfs, ignore_index=True).to_excel("gemini_resultados.xlsx", index=False)
        print("\n✨ Arquivo final gerado com sucesso!")

if __name__ == "__main__":
    process_files()
