import os
import glob
from google import genai
import google.generativeai as genai
from google.generativeai import GenerativeModel
from dotenv import load_dotenv
import zipfile

load_dotenv() 
api_key = os.getenv("GEMINI_API_KEY")
artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action") 

if not api_key:
    print("Error: The 'GEMINI_API_KEY' was not found.")
    print("Please make sure you have created a '.env' file with your key in it.")
    exit()

genai.configure(api_key=api_key)


try:
    #Path Join Artifacts
    search_pattern = os.path.join(artifact_folder, "**", "*.*")
    file_paths = [f for f in glob.glob(search_pattern, recursive=True) if os.path.isfile(f)]
    zip_path = glob.glob(file_paths, recursive=True)
    
    #Extract folders of all zipers of Artifacts
    for zip_path in file_paths:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            extract_directory = os.path.join(os.path.dirname (zip_path))
            zip_ref.extractall(file_paths)
            print(f"Extractes Folders {zip_path}, to {extract_directory}")
        
    #Scroll to File Paths in Artifacts after Download
    for paths, file, files in os.walk(file_paths):
        print(f"Pastas encontradas {paths}")
        for path in file_paths:
            path(os.path.join(paths))
        for file in files:
            file(os.path.join(file))
    with open (path, file):
        try:
            for i in file, files:
                print(f"Arquivos encontrados {i}")
                myfiles = genai.upload_file(file=file_paths)
        except:
            print("Not possible to scroll on paths and files.")
            
    #Upload maximum 4 files on Folder in Prompt Command 
    for files in enumerate(myfiles):
        files = []
        max_num = os.path.join(file=i in files, max_limit = 3)
        for max_limit in max_num:
            files_upload = myfiles()
            print(f"Arquivos upados, Nº {i}")
    
    #Model of AI
    model = genai.GenerativeModel(model_name='gemini-2.5-pro')
    
    #Prompt to Gemini.API
    prompt = f""" {len(myfiles)}
    Transforme o PDF ou Img (seja png ou jpeg) e transforme em planilha (sempre em formato markdown com tabelas e colunas separadas com o objetivo de apenas copiar para Excel).
    Leia tudo que está na imagem e coloque na planilha na seguinte ordem em colunas:
    Empresa (Nome da Empresa), Data (Com a Data Início e Data Fim, separe por -), Data Início, Data Fim, Campanha (Adicione o Nome da Campanha + Dia da Campanha que é o dia da oferta do encarte) + Estado (que é o estado do encarte), Categoria do Produto, Produto (Descrição, tire a referência do produto), Preço (Do Encarte), App (Preço para Usuários do App, se o Encarte falar), Cidade (Que mostrar no Encarte) e Estado (Que mostrar no Encarte, coloque somente a SIGLA DO ESTADO).
    Transforme uma de cada vez separando somente o que se extraiu na imagem separando em uma planilha. Sempre quando mandar novamente separe somente as informações da imagem que eu mandar.
    Sempre verifique duas vezes antes de fazer a leitura do encarte, verifique os erros antes da transformação.
    Em letras maiúsculas e minúsculas, respeitando a regra da língua portuguesa.
    """

    print("Sending prompt to the Gemini API...")
    
    response = model.generate_content(prompt)

    print("\n--- Model Response ---")
    print(response.text)
    print("----------------------")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

