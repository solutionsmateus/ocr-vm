import os
import glob
import zipfile
import time
from dotenv import load_dotenv
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

# --- 1. Configuração Inicial ---
load_dotenv()
api_key = os.getenv("GEMINI_API_KEY")
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

Primeira letra maiúscula, restante minúsculo

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
"""

# Extensões de arquivo 
VALID_EXTENSIONS = ('.jpeg', '.jpg', '.png', '.pdf')
BATCH_SIZE = 1
all_markdown_results = []

def process_files():
    """
    Função principal para executar todo o fluxo de trabalho.
    """
    
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

                uploaded_files = []
                prompt_payload = []

                for path in batch_paths:
                    try:
                        print(f"    Subindo arquivo: {os.path.basename(path)}")
                        time.sleep(1) 
                        file = genai.upload_file(path=path)
                        uploaded_files.append(file)
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
                    
                    all_markdown_results.append(response.text)
                    print(f"    Resposta recebida e armazenada.")
                
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

    if not all_markdown_results:
        print("Nenhum resultado foi gerado pela API.")
    else:
        output_filename = "gemini_resultados_compilados.md"
        print(f"Salvando {len(all_markdown_results)} planilhas em um único arquivo...")
        
        try:
            with open(output_filename, "w", encoding="utf-8") as f:
                f.write("\n\n---\n\n".join(all_markdown_results))
            print(f"\n--- SUCESSO! ---")
            print(f"Todos os arquivos foram processados.")
            print(f"Resultado salvo em: {output_filename}")
        except Exception as e:
            print(f"ERRO ao salvar o arquivo final: {e}")

if __name__ == "__main__":
    try:
        process_files()
    except Exception as e:
        print(f"Um erro inesperado e fatal ocorreu: {e}")


#def identificator_files()