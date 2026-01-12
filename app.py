import os
import glob
import zipfile
import time
from pathlib import Path
from dotenv import load_dotenv
import google.generativeai as genai
from google.genai.errors import APIError 
from google.genai.types import HarmCategory, HarmBlockThreshold, GenerateContentConfig, SafetySetting 
import pandas as pd
import io
import re 
from concurrent.futures import ThreadPoolExecutor, as_completed 
from itertools import cycle # Importa cycle para rotacionar as chaves

load_dotenv()

# --- Configuração de Chaves e Variáveis de Ambiente ---
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
# --- Fim da Configuração de Chaves e Variáveis de Ambiente ---


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

# --- PROMPT INALTERADO ---
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

Formato: Nome da campanha + Data do Encarte + Estado

Nunca colocar campanha dentro da coluna Empresa.

✅ PRODUTO

Sem referência/código (ex.: “cx”, “ref”, SKU, código interno)

Se o nome estiver incompleto, não inventar.

Preciso de todas as informações que estiverem na imagem de cada produto.

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

Siga estes detalhes minunciosamente: 
DETALHE 1: : QUANDO FOR ENCARTES DO COMETA SUPERMERCADOS, A CIDADE E LOJA SEMPRE VÃO SER “FORTALEZA” E O ESTADO: CEARÁ

DETALHE 2: QUANDO FOR ENCARTES DO NOVO ATACAREJO, A LOJA SEMPRE VAI SER "Olinda", A CIDADE: "Recife" E O ESTADO: "PERNAMBUCO"

DETALHE 3: LEIA A DESCRIÇÃO COMPLETA DOS PRODUTOS DOS ENCARTES DE TODOS OS SUPERMERCADOS, EU PRECISO DE TODAS AS INFORMAÇÕES CORRETAS NOS SEUS LUGARES DEVIDOS DE ACORDO COM AS CATEGORIAS CITADAS ACIMA.

**AVISO CRÍTICO**: NÃO utilize o caractere PIPE (|) dentro de NENHUM campo de texto ou dado. Se precisar de separador, use vírgula ou ponto-e-vírgula.
"""
# --- FIM DO PROMPT INALTERADO ---


VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
BATCH_SIZE = 1 
MAX_THREADS = 8 

all_markdown_results = []
all_dataframes = [] 


def parse_markdown_table(markdown_text):
    COLUMNS = [
        "Empresa", "Data", "Data Início", "Data Fim", "Campanha", 
        "Categoria do Produto", "Produto", "Medida", "Quantidade", 
        "Preço", "App", "Loja", "Cidade", "Estado"
    ]
    
    try:
        lines = markdown_text.strip().split('\n')
        # Filtra linhas que começam com '|' e exclui as duas primeiras linhas (header e separador)
        data_lines = [line for line in lines if line.strip().startswith('|')][2:]
        
        cleaned_data = '\n'.join(data_lines)
        data = io.StringIO(cleaned_data)
        
        # Leitura robusta da tabela Markdown
        df = pd.read_csv(
            data, 
            sep='|', 
            skipinitialspace=True, 
            header=None,
            on_bad_lines='warn',
            engine='python' 
        )
        
        # Remove a primeira e a última coluna (que são separadores vazios)
        if df.shape[1] >= 2:
            df = df.iloc[:, 1:-1]
        
        # Tratamento de colunas faltantes/extras
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
                # Adiciona colunas faltantes com None
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


# 💡 FUNÇÃO OTIMIZADA: Tenta a sequência UPLOAD -> GENERATE -> DELETE com várias chaves em caso de falha.
def process_single_file(file_path, key_iterator):
    uploaded_file = None
    
    # 🔄 Loop de Failover dentro da Thread
    for i in range(len(API_KEY_LIST)):
        api_key = next(key_iterator)
        key_name = f"Chave de Failover #{i + 1}"
        
        try:
            # 1. Configura o cliente para esta tentativa
            client = genai.Client(api_key=api_key)
            model = client.models.GenerativeModel(
                model_name=MODEL_NAME, 
                safety_settings=safety_settings_list
            )
            
            # 2. Upload
            print(f"[THREAD] Tentando UP/GEN/DEL com {key_name} para {os.path.basename(file_path)}")
            time.sleep(0.5) 
            
            # Upload usa o cliente específico da chave atual
            uploaded_file = client.files.upload(file=Path(file_path)) 
            
            # 3. Geração de Conteúdo
            prompt_payload = [
                f"1 arquivo anexado ({os.path.basename(file_path)}).",
                PROMPT_TEXT,
                uploaded_file
            ]
            
            config = GenerateContentConfig(
                safety_settings=safety_settings_list
            )
            
            # A geração usa o modelo específico da chave atual
            response = model.generate_content(
                contents=prompt_payload,
                config=config, 
            )
            
            # 4. Parsing e Sucesso
            df = parse_markdown_table(response.text)
            if df is not None:
                print(f"[THREAD] SUCESSO na conversão para DataFrame de {os.path.basename(file_path)} com {key_name}.")
                return df
            else:
                print(f"[THREAD] Falha de conversão: {os.path.basename(file_path)}. Tentar próxima chave.")
                continue # Tenta a próxima chave se a conversão falhar

        except APIError as e:
            if "RESOURCE_EXHAUSTED" in str(e) or "429" in str(e):
                print(f"[THREAD] ERRO de COTA (429 RESOURCE_EXHAUSTED) com {key_name}.")
                retry_delay = 15 
                match = re.search(r"'retryDelay': '(\d+)s'", str(e))
                if match:
                    retry_delay = int(match.group(1)) + 1 
                
                print(f"[THREAD] Aguardando {retry_delay} segundos antes de tentar a próxima chave...")
                time.sleep(retry_delay) 
                # Continua o loop para tentar a próxima chave
            
            elif "PERMISSION_DENIED" in str(e) or "403" in str(e):
                # Este erro pode ocorrer se houver falha no upload/acesso ao arquivo. 
                # É crucial tentar a próxima chave.
                print(f"[THREAD] ERRO FATAL (403 PERMISSION_DENIED) com {key_name}: Arquivo não pode ser acessado. Tentando próxima chave.")
                time.sleep(5) # Espera um pouco antes de tentar o próximo cliente
            
            else:
                print(f"[THREAD] ERRO INESPERADO da API com {key_name}: {e}. Tentando próxima chave.")
                time.sleep(5)
            
            continue # Tenta a próxima chave

        except Exception as e:
            print(f"[THREAD] ERRO geral (Upload ou Conexão) com {key_name} para {os.path.basename(file_path)}: {e}. Tentando próxima chave.")
            time.sleep(5)
            continue # Tenta a próxima chave
        
        finally:
            if uploaded_file:
                # 5. Deleção (CRÍTICO: Usa o cliente que FEZ o upload, que é o 'client' atual)
                print(f"[THREAD] Limpando arquivo {uploaded_file.name} do servidor Gemini...")
                try:
                    time.sleep(0.5) 
                    client.files.delete(name=uploaded_file.name)
                    uploaded_file = None # Reseta o arquivo upado para a próxima tentativa
                except Exception as e:
                    # Se o erro for 403, pode ser que o upload não tenha funcionado, ou o delete falhou.
                    print(f"[THREAD] Erro ao deletar {uploaded_file.name} com {key_name}: {e}")
                    # Não levantamos exceção aqui, pois a tarefa já falhou ou teve sucesso.
    
    # Se o loop terminar sem sucesso
    print(f"[THREAD] FALHA TOTAL: Não foi possível processar {os.path.basename(file_path)} após {len(API_KEY_LIST)} tentativas de failover.")
    return None

def process_files():
    print(f"Procurando por arquivos .zip em {artifact_folder}...")
    zip_pattern = os.path.join(artifact_folder, "**", "*.zip")
    zip_files = glob.glob(zip_pattern, recursive=True)

    # ... (Lógica de Extração de Zips inalterada) ...
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
    all_file_paths = []
    
    for root, dirs, files in os.walk(artifact_folder, topdown=False):
        if not dirs and files and root != artifact_folder:
            file_paths_to_process = [
                os.path.join(root, f) for f in files if f.lower().endswith(VALID_EXTENSIONS)
            ]
            all_file_paths.extend(file_paths_to_process)

    if not all_file_paths:
        print("Nenhum arquivo válido encontrado para processamento.")
        return

    print(f"TOTAL: {len(all_file_paths)} arquivos encontrados para processar.")
    print(f"Processando em paralelo com até {MAX_THREADS} threads...")

    # 💡 MUDANÇA CRÍTICA: Criar um iterador cíclico de chaves para distribuição inicial
    key_cycle = cycle(API_KEY_LIST)
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Passa o iterador cíclico para cada thread, garantindo rotação
        future_to_file = {executor.submit(process_single_file, path, key_cycle): path for path in all_file_paths}
        
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                df_result = future.result() 
                if df_result is not None:
                    all_dataframes.append(df_result) 
                
            except Exception as exc:
                print(f"Arquivo {os.path.basename(file_path)} gerou uma exceção: {exc}")

    
    if not all_dataframes:
        print("Nenhum resultado foi gerado pela API ou convertido para DataFrame.")
    else:
        save_dataframes_to_excel(all_dataframes)


if __name__ == "__main__":
    try:
        # ... (Verificação de dependências inalterada) ...
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