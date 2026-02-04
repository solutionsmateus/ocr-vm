import os
import glob
import zipfile
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
# Importação das configurações e tipos de segurança
from google.genai.types import HarmCategory, HarmBlockThreshold, GenerateContentConfig, SafetySetting
import pandas as pd
import io

# --- 1. Configuração Inicial ---
load_dotenv()
api_key = os.environ.get("GEMINI_API_KEY")
artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")

if not api_key:
    print("Erro: A 'GEMINI_API_KEY' não foi encontrada nas variáveis de ambiente.")
    print("Por favor, verifique se o secret está configurado e se o YAML o injeta corretamente.")
    exit()

client = genai.Client(api_key=api_key)

# Configuração de Segurança
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
Assaí Atacadista, Atacadão, Cometa Supermercados, Frangolândia, GBarbosa, Atakarejo, Novo Atakarejo
Se o encarte tiver outra empresa → deixar em branco.

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
Detectar apenas as unidades: g, mg, kg, litro, cm, metro (se não houver medida, deixar vazio)

✅ QUANTIDADE
1 quando for item unitário. Se for pack/kit/leve X/caixa → usar o número total de unidades.

✅ LOJA (IMPORTANTE)
Deve conter todas as CIDADES onde o encarte atua. Separar múltiplas cidades com "; ".
Ex.: São Luís; Imperatriz; Bacabal

✅ CIDADE (IMPORTANTE)
Deve ser apenas a cidade padrão do estado:
MARANHÃO -> São Luís
CEARÁ -> Fortaleza
PARÁ -> Belém
PERNAMBUCO -> Recife
ALAGOAS -> Maceió
SERGIPE -> Aracaju
BAHIA -> Salvador
PIAUÍ -> Teresina
PARAÍBA -> João Pessoa

✅ ESTADO
Nome por extenso e EM MAIÚSCULAS (ex: MARANHÃO)

✅ PADRÕES GERAIS
Nunca duplicar itens. Não inventar dados.
AVISO CRÍTICO: NÃO utilize o caractere PIPE (|) dentro de NENHUM campo de texto.
"""

# Extensões de arquivo 
VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
BATCH_SIZE = 1
all_markdown_results = []
all_dataframes = [] 

def parse_markdown_table(markdown_text):
    # Nomes EXATOS das 14 colunas
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
            on_bad_lines='warn',
            engine='python' 
        )
        
        df = df.iloc[:, 1:-1]
        
        if df.shape[1] == len(COLUMNS):
            df.columns = COLUMNS
        else:
            print(f"AVISO CRÍTICO: Colunas esperadas ({len(COLUMNS)}) != Colunas detectadas ({df.shape[1]}). Aplicando reajuste.")
            if df.shape[1] > len(COLUMNS):
                df = df.iloc[:, :len(COLUMNS)]
                df.columns = COLUMNS
            else:
                missing_cols = len(COLUMNS) - df.shape[1]
                for i in range(missing_cols):
                    df[f'COL_MISSING_{i}'] = None
                df.columns = COLUMNS
            
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
    # 2. Extrair todos os Zips
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
                
                # Pausa preventiva entre lotes
                time.sleep(2)

                uploaded_files = []
                prompt_payload = []

                # Upload dos arquivos
                for path in batch_paths:
                    try:
                        print(f"    Subindo arquivo: {os.path.basename(path)}") 
                        file = client.files.upload(file=Path(path))
                        uploaded_files.append(file)
                        # Pausa pequena após upload para garantir propagação
                        time.sleep(2)
                    except Exception as e:
                        print(f"    ERRO ao subir {path}: {e}")
                
                if not uploaded_files:
                    print("    Nenhum arquivo foi upado com sucesso neste lote. Pulando.")
                    continue

                prompt_payload = [
                    f"{len(uploaded_files)} arquivos anexados.",
                    PROMPT_TEXT
                ] + uploaded_files

                # --- INÍCIO DA LÓGICA DE RETRY (BACKOFF) ---
                max_retries = 5
                base_delay = 10 # Começa esperando 10 segundos em caso de erro
                
                response = None
                
                for attempt in range(max_retries):
                    try:
                        print(f"    Enviando para o Gemini (Tentativa {attempt + 1}/{max_retries})...")
                        
                        config = GenerateContentConfig(
                            safety_settings=safety_settings_list
                        )
                        
                        response = client.models.generate_content(
                            model=MODEL_NAME,
                            contents=prompt_payload,
                            config=config, 
                        )
                        
                        # Se chegou aqui, funcionou! Sai do loop de retry.
                        break 
                        
                    except Exception as e:
                        error_str = str(e).lower()
                        # Verifica erros comuns de limite (429 ou Quota)
                        if "429" in error_str or "too many requests" in error_str or "quota" in error_str or "exhausted" in error_str:
                            wait_time = base_delay * (2 ** attempt) # Ex: 10s, 20s, 40s, 80s...
                            print(f"LIMITE ATINGIDO (429/Quota). Aguardando {wait_time} segundos antes de tentar novamente...")
                            time.sleep(wait_time)
                        else:
                            # Se for outro erro (ex: prompt inválido), não adianta tentar de novo
                            print(f"    ❌ Erro fatal na API (não é taxa): {e}")
                            break
                
                # --- FIM DA LÓGICA DE RETRY ---

                # Processamento da resposta (se houve sucesso)
                if response:
                    try:
                        df = parse_markdown_table(response.text)
                        if df is not None:
                            all_dataframes.append(df)
                            print(f"Resposta recebida e convertida em DataFrame.")
                        else:
                            print(f"Falha na conversão Markdown -> DataFrame.")
                    except Exception as e:
                         print(f"    Erro ao processar resposta: {e}")
                else:
                    print("    Grave: Não foi possível obter resposta após todas as tentativas.")

                # Limpeza (Sempre executa)
                print("    Limpando arquivos do servidor Gemini...")
                for file in uploaded_files:
                    try:
                        client.files.delete(name=file.name)
                        time.sleep(1) 
                    except Exception as e:
                        print(f"    Erro ao deletar arquivo {file.name}: {e}")
            
            print(f"--- Diretório {root} concluído ---\n")

    if not all_dataframes:
        print("Nenhum resultado foi gerado pela API.")
    else:
        save_dataframes_to_excel(all_dataframes)


if __name__ == "__main__":
    try:
        import pandas as pd
        import openpyxl 
    except ImportError:
        print("\n--- DEPENDÊNCIA FALTANDO ---")
        print("pip install pandas openpyxl")
        exit()

    try:
        process_files()
    except Exception as e:
        print(f"Um erro inesperado e fatal ocorreu: {e}")
