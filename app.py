import os
import glob
import zipfile
import time
from dotenv import load_dotenv
from google import genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import pandas as pd
import io

# --- 1. Configuração Inicial ---
load_dotenv()
api_key = "AIzaSyA5_hcdJ4Ys4hkN3UraWMm2sjT3sLWkhoQ"
artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")

if not api_key:
    print("Erro: A 'GEMINI_API_KEY' não foi encontrada.")
    print("Por favor, crie um arquivo '.env' com sua chave.")
    exit()

try:
    genai.configure(api_key=api_key)
except Exception as e:
    print(f"Erro ao configurar a API Gemini: {e}")
    exit()

safety_settings = {
    HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
    HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
}

model = genai.GenerativeModel(
    model_name='gemini-flash-latest', 
    safety_settings=safety_settings
)

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

Se o encarte tiver outra empresa → deixar em branco (nunca inventar).

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
        
        # Tenta ler a tabela. Usamos 'header=None' e 'engine='python'' para maior tolerância.
        df = pd.read_csv(
            data, 
            sep='|', 
            skipinitialspace=True, 
            header=None,
            on_bad_lines='warn', # Avisa sobre linhas problemáticas, mas tenta continuar
            engine='python' 
        )
        
        # Limpeza pós-leitura
        # Remove a primeira e a última coluna (vazias devido ao formato |col1|col2|)
        df = df.iloc[:, 1:-1]
        
        # Define os nomes das colunas
        if df.shape[1] == len(COLUMNS):
            df.columns = COLUMNS
        else:
            print(f"AVISO CRÍTICO: Colunas esperadas ({len(COLUMNS)}) != Colunas detectadas ({df.shape[1]}). Aplicando reajuste forçado.")
            # Se o número de colunas não bater, tentamos prosseguir descartando colunas extras
            if df.shape[1] > len(COLUMNS):
                df = df.iloc[:, :len(COLUMNS)]
                df.columns = COLUMNS
                print("Reajuste forçado aplicado: colunas extras descartadas.")
            else:
                 # Se houver menos colunas, preenchemos com NaN no final
                missing_cols = len(COLUMNS) - df.shape[1]
                for i in range(missing_cols):
                    df[f'COL_MISSING_{i}'] = None
                df.columns = COLUMNS
                print("Reajuste forçado aplicado: colunas faltantes adicionadas.")
            
        # Remove linhas que são todas NaN (podem ser linhas vazias residuais)
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

                for path in batch_paths:
                    try:
                        print(f"    Subindo arquivo: {os.path.basename(path)}") 
                        file = genai.upload_file(path=path)
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
                    response = model.generate_content(prompt_payload)
                    
                    # NOVO: Converte a resposta Markdown para DataFrame e armazena
                    df = parse_markdown_table(response.text)
                    if df is not None:
                        all_dataframes.append(df)
                        print(f"    Resposta recebida e convertida em DataFrame.")
                    else:
                        # Se a conversão falhar, ainda tentamos printar a resposta bruta para debug
                        print(f"Resposta bruta do Gemini (pode conter erro de formatação):")
                        print("--- INÍCIO DA RESPOSTA BRUTA ---")
                        print(response.text)
                        print("--- FIM DA RESPOSTA BRUTA ---")
                        print(f"    Resposta recebida, mas falhou na conversão para DataFrame.")
                    
                except Exception as e:
                    print(f"    ERRO ao chamar a API Gemini: {e}")
                
                finally:
                    print("    Limpando arquivos do servidor Gemini...")
                    for file in uploaded_files:
                        try:
                            time.sleep(1) # Pausa para evitar limite de taxa
                            genai.delete_file(file.name)
                        except Exception as e:
                            print(f"    Erro ao deletar arquivo {file.name}: {e}")
            
            print(f"--- Diretório {root} concluído ---\n")

    if not all_dataframes:
        print("Nenhum resultado foi gerado pela API ou convertido para DataFrame.")
    else:
        # NOVO: Chamada para salvar em XLSX
        save_dataframes_to_excel(all_dataframes)


if __name__ == "__main__":
    # Verificação de dependências
    try:
        import pandas as pd
        import openpyxl # openpyxl é o motor padrão para escrita de XLSX pelo pandas
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