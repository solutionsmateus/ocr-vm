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

load_dotenv()

API_KEY_LIST = []
key_primary = os.environ.get("GEMINI_API_KEY")
if key_primary:
    API_KEY_LIST.append(key_primary)

key_backup_1 = os.environ.get("GEMINI_API_KEY_BACKUP_01")
if key_backup_1:
    API_KEY_LIST.append(key_backup_1)

key_backup_2 = os.environ.get("GEMINI_API_KEY_BACKUP_02")

if key_backup_2:
    API_KEY_LIST.append(key_backup_2)

if not API_KEY_LIST:
    print("Erro: Nenhuma chave de API (GEMINI_API_KEY_PRIMARY ou BACKUP) foi encontrada nas variáveis de ambiente.")
    print("Por favor, verifique se os secrets estão configurados no GitHub e injetados no YAML.")
    exit()

artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")


safety_settings_list = [
    SafetySetting(
        category=HarmCategory.HARM_CATEGORY_HARASSMENT,
        threshold=HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=HarmCategory.HARM_CATEGORY_HATE_SPEECH,
        threshold=HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
        threshold=HarmBlockThreshold.BLOCK_NONE
    ),
    SafetySetting(
        category=HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
        threshold=HarmBlockThreshold.BLOCK_NONE
    ),
]

MODEL_NAME = 'gemini-2.5-flash' 

PROMPT_TEXT = """
Transforme o PDF/PNG/JPEG em tabela Markdown (para copiar no Excel) e XLSX, usando esta ordem EXATA de colunas:

Empresa, Data, Data Início, Data Fim, Campanha, Categoria do Produto, Produto, Medida, Quantidade, Preço, App, Loja, Cidade, Estado.

✅ REGRAS OBRIGATÓRIAS
✅ EMPRESA

Nunca substituir supermercado pela campanha.

Permitir apenas estes valores:

Assaí Atacadista

Atacadão

Cometa Supermercados

Frangolândia

GBarbosa

Atakarejo

Novo Atakarejo

Se o encarte tiver outra empresa → deixar em branco (never inventar).

✅ DATA

Data = “Data Início - Data Fim” (DD/MM/AAAA)

Data Início e Data Fim também devem aparecer separadamente.

✅ CAMPANHA

Formato: Nome da campanha + dia da oferta + Estado

Nunca colocar campanha dentro da coluna Empresa.

✅ PRODUTO

Sem referência/código (ex.: “cx”, “ref”, SKU, código interno)

Se o nome estiver incompleto, não inventar.

✅ MEDIDA

Detectar apenas as unidades:

g, mg, kg, litro, cm, metro
(se não houver medida, deixar vazio)

✅ QUANTIDADE

1 quando for item unitário.

Se for pack/kit/leve X/caixa → usar o número total de unidades.

✅ LOJA (IMPORTANTE)

Deve conter todas as CIDADES onde o encarte atua

Separar múltiplas cidades com "; "

Sempre com acentuação e ortografia correta:

Primeira letra maiúscula, restante minúscula

Ex.: São Luís; Imperatriz; Bacabal; Maceió; Arapiraca

✅ CIDADE (IMPORTANTE)

Deve ser apenas a cidade padrão do estado, mesmo que haja várias lojas:

ESTADO (MAIÚSCULO)	Cidade padrão (capitalizada corretamente)
MARANHÃO	São Luís
CEARÁ	Fortaleza
PARÁ	Belém
PERNAMBUCO	Recife
ALAGOAS	Maceió
SERGIPE	Aracaju
BAHIA	Salvador
PIAUÍ	Teresina
PARAÍBA	João Pessoa
✅ ESTADO

Nome por extenso e EM MAIÚSCULAS

Ex.: MARANHÃO, CEARÁ, PARÁ, PERNAMBUCO, ALAGOAS, SERGIPE, BAHIA…

✅ PADRÕES GERAIS

Nunca duplicar itens

Não inventar dados — se não estiver no encarte, deixar em branco

Corrigir acentos, erros de OCR e números

Extrair somente o que existe na imagem

**AVISO CRÍTICO**: NÃO utilize o caractere PIPE (|) dentro de NENHUM campo de texto ou dado. Se precisar de separador, use vírgula ou ponto-e-vírgula.
"""

# Extensões de arquivo 
VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
BATCH_SIZE = 1
all_markdown_results = []
all_dataframes = [] 


def call_gemini_api_with_failover(prompt_payload, config):
    """
    Tenta chamar a API Gemini usando as chaves de API disponíveis em API_KEY_LIST.
    Alterna para a próxima chave em caso de erro 429 RESOURCE_EXHAUSTED.
    """
    
    for i, api_key in enumerate(API_KEY_LIST):
        key_name = f"Chave #{i + 1}"
        
        try:
            client = genai.Client(api_key=api_key)
            print(f"    Tentando chamar API com {key_name}...")
            
            response = client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt_payload,
                config=config, 
            )
            
            print(f"    SUCESSO na chamada API com {key_name}.")
            return response

        except APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e):
                print(f"    ERRO de COTA (429 RESOURCE_EXHAUSTED) com {key_name}.")
                
                retry_delay = 15 
                match = re.search(r"'retryDelay': '(\d+)s'", str(e))
                if match:
                    retry_delay = int(match.group(1)) + 1 # Adiciona 1s de buffer
                
                print(f"    Aguardando {retry_delay} segundos antes de tentar a próxima chave...")
                time.sleep(retry_delay)
                continue 
            
            else:
                print(f"    ERRO INESPERADO da API com {key_name}: {e}")
                raise e 

        except Exception as e:
            print(f"    ERRO geral ao conectar ou processar com {key_name}: {e}")
            raise e
            
    raise Exception("Falha ao chamar a API Gemini: Todas as chaves esgotaram a cota (429) ou falharam.")


def parse_markdown_table(markdown_text):
    COLUMNS = [
        "Empresa", "Data", "Data Início", "Data Fim", "Campanha", 
        "Categoria do Produto", "Produto", "Medida", "Quantidade", 
        "Preço", "App", "Loja", "Cidade", "Estado"
    ]
    
    try:
        lines = markdown_text.strip().split('\n')
        data_lines = [line for line in lines[2:] if line.strip().startswith('|')]
        
        cleaned_data = '\n'.join(data_lines)
        data = io.StringIO(cleaned_data)
        
        df = pd.read_csv(
            data, 
            sep='|', 
            skipinitialspace=True, 
            header=None,
            on_bad_lines='warn', # Avisa sobre linhas problemáticas, mas tenta continuar
            engine='python' 
        )
        
        df = df.iloc[:, 1:-1]
        
        if df.shape[1] == len(COLUMNS):
            df.columns = COLUMNS
        else:
            print(f"AVISO CRÍTICO: Colunas esperadas ({len(COLUMNS)}) != Colunas detectadas ({df.shape[1]}). Aplicando reajuste forçado.")
            if df.shape[1] > len(COLUMNS):
                df = df.iloc[:, :len(COLUMNS)]
                df.columns = COLUMNS
                print("Reajuste forçado aplicado: colunas extras descartadas.")
            else:
                missing_cols = len(COLUMNS) - df.shape[1]
                for i in range(missing_cols):
                    df[f'COL_MISSING_{i}'] = None
                df.columns = COLUMNS
                print("Reajuste forçado aplicado: colunas faltantes adicionadas.")
            
        df.dropna(how='all', inplace=True)
        
        return df
        
    except Exception as e:
        print(f"AVISO: Não foi possível converter a tabela Markdown em DataFrame. Erro: {e}")
        return None

def save_dataframes_to_excel(dataframes, output_filename="gemini_resultados_compilados.xlsx"):
    if not dataframes:
        print("Nenhum DataFrame para salvar.")
        return

    try:
        final_df = pd.concat(dataframes, ignore_index=True)
        
        final_df.to_excel(output_filename, index=False, engine='openpyxl')
        
        print(f"SUCESSO!")
        print(f"Todos os arquivos foram processados.")
        print(f"Resultado salvo em: {output_filename}")
    except Exception as e:
        print(f"ERRO ao salvar o arquivo final XLSX: {e}")

def process_files():
    print(f"Procurando por arquivos .zip em {artifact_folder}...")
    zip_pattern = os.path.join(artifact_folder, "**", "*.zip")
    zip_files = glob.glob(zip_pattern, recursive=True)

    if not zip_files:
        print("Nenhum arquivo .zip encontrado. Verificando arquivos existentes...")
    else:
        print(f"Encontrados {len(zip_files)} arquivos .zip. Extraindo...")
        for zip_path in zip_files:
            try:
                extract_directory = os.path.dirname(zip_path)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_directory)
                print(f"Extraído: {zip_path} -> {extract_directory}")
            except zipfile.BadZipFile:
                print(f"Erro: {zip_path} não é um arquivo zip válido ou está corrompido.")
            except Exception as e:
                print(f"Erro ao extrair {zip_path}: {e}")
        print("Extração de Zips concluída.\n")

    print("Iniciando varredura das pastas de supermercados...")
    
    for root, dirs, files in os.walk(artifact_folder, topdown=False):
        
        if not dirs and files and root != artifact_folder:
            
            file_paths_to_process = [
                os.path.join(root, f) for f in files if f.lower().endswith(VALID_EXTENSIONS)
            ]

            if not file_paths_to_process:
                continue 

            print(f"--- Processando Diretório: {root} ---")
            print(f"Encontrados {len(file_paths_to_process)} arquivos válidos.")

            for i in range(0, len(file_paths_to_process), BATCH_SIZE):
                batch_paths = file_paths_to_process[i : i + BATCH_SIZE]
                print(f"  Processando lote {i//BATCH_SIZE + 1} ({len(batch_paths)} arquivos)...")
                time.sleep(1)

                uploaded_files = []
                prompt_payload = []
                
                try:
                    upload_client = genai.Client(api_key=API_KEY_LIST[0]) 
                except Exception as e:
                    print(f"ERRO: Não foi possível criar o cliente de upload com a primeira chave disponível. {e}")
                    continue

                for path in batch_paths:
                    try:
                        print(f"    Subindo arquivo: {os.path.basename(path)}") 
                        file = upload_client.files.upload(file=Path(path))
                        uploaded_files.append(file)
                        time.sleep(1)
                    except Exception as e:
                        print(f"    ERRO ao subir {path}: {e}")
                
                if not uploaded_files:
                    print("    Nenhum arquivo foi upado com sucesso neste lote. Pulando.")
                    continue

                prompt_payload = [
                    f"{len(uploaded_files)} arquivos anexados.",
                    PROMPT_TEXT
                ] + uploaded_files

                try:
                    print(f"    Enviando {len(uploaded_files)} arquivos para o Gemini...")
                    
                    config = GenerateContentConfig(
                        safety_settings=safety_settings_list
                    )
                    
                    response = call_gemini_api_with_failover(prompt_payload, config)
                    
                    df = parse_markdown_table(response.text)
                    if df is not None:
                        all_dataframes.append(df)
                        print(f"    Resposta recebida e convertida em DataFrame.")
                    else:
                        print(f"Resposta bruta do Gemini (pode conter erro de formatação):")
                        print("--- INÍCIO DA RESPOSTA BRUTA ---")
                        print(response.text)
                        print("--- FIM DA RESPOSTA BRUTA ---")
                        print(f"    Resposta recebida, mas falhou na conversão para DataFrame.")
                    
                except Exception as e:
                    print(f"    ERRO FATAL (todas as tentativas falharam): {e}")
                
                finally:
                    print("    Limpando arquivos do servidor Gemini...")
                    for file in uploaded_files:
                        try:
                            time.sleep(1) # Pausa para evitar limite de taxa
                            upload_client.files.delete(name=file.name)
                        except Exception as e:
                            print(f"    Erro ao deletar arquivo {file.name}: {e}")
            
            print(f"--- Diretório {root} concluído ---\n")

    if not all_dataframes:
        print("Nenhum resultado foi gerado pela API ou convertido para DataFrame.")
    else:
        save_dataframes_to_excel(all_dataframes)


if __name__ == "__main__":
    try:
        import pandas as pd
        import openpyxl 
    except ImportError:
        print("\n--- DEPENDÊNCIA FALTANDO ---")
        print("Para salvar em XLSX, você precisa instalar pandas e openpyxl.")
        print("Execute o comando:")
        print("pip install pandas openpyxl")
        exit()

    try:
        process_files()
    except Exception as e:
        print(f"Um erro inesperado e fatal ocorreu: {e}")