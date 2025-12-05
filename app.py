import os
import glob
import zipfile
import time
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold
import pandas as pd
import io

# --- 1. Configuração Inicial ---
load_dotenv()
artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")

# 💡 NOVO: Carrega as chaves dedicadas e a chave padrão (fallback) do ambiente
DEFAULT_API_KEY = os.getenv("GEMINI_API_KEY")

# Mapeamento de Chaves de Supermercado (O nome do token deve ser parte do nome da pasta)
# Ex: A pasta "Assaí Atacadista" usa a chave ASSAI_KEY.
KEY_MAPPING = {
    "ASSAI": os.getenv("ASSAI_KEY"),
    "ATACADAO": os.getenv("ATACADAO_KEY"),
    "ATAKAREJO": os.getenv("ATAKAREJO_KEY"),
    "COMETA": os.getenv("COMETA_KEY"),
    "FRANGOLANDIA": os.getenv("FRANGOLANDIA_KEY"),
    "GBARBOSA": os.getenv("GBARBOSA_KEY"),
    "NOVO_ATACAREJO": os.getenv("NOVO_ATACAREJO_KEY"),
}

# Limpa o mapeamento removendo chaves vazias e garante o uso em maiúsculas para busca
CLEANED_KEY_MAPPING = {k: v for k, v in KEY_MAPPING.items() if v}

if not DEFAULT_API_KEY and not CLEANED_KEY_MAPPING:
    print("Erro: Nenhuma chave API Gemini (padrão ou dedicada) foi encontrada. Saindo.")
    exit()

# 💡 NOVO: Função para configurar o cliente Gemini de forma dinâmica
def get_gemini_model(api_key):
    """Configura o cliente Gemini com a chave fornecida e retorna a instância do modelo."""
    if not api_key:
        raise ValueError("Chave API não fornecida.")
    
    # ⚠️ Esta linha reconfigura a API GLOBALMENTE para o processo atual
    genai.configure(api_key=api_key) 
    
    safety_settings = {
        HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
        HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
    }

    # O objeto model retornado usará a configuração mais recente
    return genai.GenerativeModel(
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

g, mg, kg, litro, cm, metro, ou unid (se o produto não tiver nenhuma medida).
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
BAHIA	Vitória da Conquista ou Salvador
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

🛑 **AVISO CRÍTICO**: NÃO utilize o caractere PIPE (|) dentro de NENHUM campo de texto ou dado. Se precisar de separador, use vírgula ou ponto-e-vírgula.
"""

# Extensões de arquivo 
VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
BATCH_SIZE = 1
all_dataframes = [] 

def parse_markdown_table(markdown_text):
    """
    Analisa a string de tabela Markdown e a converte em um DataFrame do pandas.
    """
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
    """
    Compila todos os DataFrames em um único arquivo XLSX.
    """
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
    """
    Função principal para executar todo o fluxo de trabalho.
    """
    global all_dataframes # Para garantir que a lista seja modificada globalmente

    # Inicializa com a primeira chave disponível
    current_api_key = next(iter(CLEANED_KEY_MAPPING.values()), DEFAULT_API_KEY)
    
    if not current_api_key:
        print("Erro: Nenhuma chave API disponível para começar. Saindo.")
        exit()
        
    try:
        current_model = get_gemini_model(current_api_key)
        print(f"Configuração inicial com a chave: {'DEDICADA' if current_api_key != DEFAULT_API_KEY else 'PADRÃO (Fallback)'}.")
    except Exception as e:
        print(f"Erro inicial ao configurar a primeira chave: {e}. Saindo.")
        exit()


    # 2. Extrair todos os Zips (lógica inalterada)
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
            
            # --- 💡 Lógica de Rotação de Chaves ---
            # 1. Normaliza o nome da pasta para busca (ex: "Assaí Atacadista" -> "ASSAIATACADISTA")
            supermarket_folder_name = os.path.basename(root).upper().replace(" ", "").replace("-", "")
            selected_key = None
            
            # 2. Busca a chave no mapeamento (ex: se o nome da pasta contém "ASSAI")
            for key_name, api_key_value in CLEANED_KEY_MAPPING.items():
                if key_name in supermarket_folder_name:
                    selected_key = api_key_value
                    break
            
            # 3. Define a chave a ser usada: dedicada ou padrão (fallback)
            key_to_use = selected_key if selected_key else DEFAULT_API_KEY
            
            # 4. Reconfigura o cliente SOMENTE se a chave for diferente da que está sendo usada
            if key_to_use and key_to_use != current_api_key:
                try:
                    current_model = get_gemini_model(key_to_use)
                    current_api_key = key_to_use
                    key_source = "DEDICADA" if selected_key else "PADRÃO (Fallback)"
                    print(f"🔑 Chave API alterada para: {os.path.basename(root)} ({key_source}).")
                except Exception as e:
                    print(f"⚠️ Erro ao configurar nova chave para {os.path.basename(root)}: {e}. Mantendo a chave anterior.")
                    # Tenta fallback para a chave padrão se a dedicada falhar (se houver)
                    if current_api_key != DEFAULT_API_KEY and DEFAULT_API_KEY:
                        current_model = get_gemini_model(DEFAULT_API_KEY)
                        current_api_key = DEFAULT_API_KEY
                        print("Tentativa de fallback para Chave Padrão.")
            # --- Fim da Lógica de Rotação ---

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
                        # O upload usa a configuração da API mais recente
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
                    # A chamada generate_content usa o 'current_model'
                    response = current_model.generate_content(prompt_payload) 
                    
                    df = parse_markdown_table(response.text)
                    if df is not None:
                        all_dataframes.append(df)
                        print(f"    Resposta recebida e convertida em DataFrame.")
                    else:
                        print(f"    Resposta bruta do Gemini (pode conter erro de formatação):")
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
                            time.sleep(1) 
                            genai.delete_file(file.name)
                        except Exception as e:
                            print(f"    Erro ao deletar arquivo {file.name}: {e}")
            
            print(f"--- Diretório {root} concluído ---\n")

    if not all_dataframes:
        print("Nenhum resultado foi gerado pela API ou convertido para DataFrame.")
    else:
        save_dataframes_to_excel(all_dataframes)


if __name__ == "__main__":
    # Verificação de dependências
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