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
Transforme o PDF ou Img (seja png ou jpeg) e transforme em planilha (sempre em formato markdown com tabelas e colunas separadas com o objetivo de apenas copiar para Excel). MUITO IMPORTANTE: Cada dado extraído deve estar em sua própria coluna/célula, usando o separador | do Markdown. Leia tudo que está na imagem e coloque na planilha na seguinte ordem em colunas: Empresa (Nome da Empresa), Data (Com a Data Início e Data Fim, separe por -), Data Início, Data Fim, Campanha (Adicione o Nome da Campanha + Dia da Campanha que é o dia da oferta do encarte) + Estado (que é o estado do encarte), Categoria do Produto, Produto (Descrição, tire a referência do produto), Preço (Do Encarte), App (Preço para Usuários do App, se o Encarte falar), Cidade (A cidade que mostrar no Encarte, MUITO IMPORTANTE COLOCAR) e Estado (Que mostrar no Encarte, coloque somente a SIGLA DO ESTADO). Transforme uma de cada vez separando somente o que se extraiu na imagem separando em uma planilha. Sempre quando mandar novamente separe somente as informações da imagem que eu mandar. Sempre verifique duas vezes antes de fazer a leitura do encarte, verifique as erros antes da transformação. Em letras maiúsculas e minúsculas, respeitando a regra da língua portuguesa.
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
