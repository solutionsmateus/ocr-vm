import os
import google.generativeai as genai
from dotenv import load_dotenv

# --- 1. Load API Key from .env file ---
# This is the most reliable way to load your API key.
# It looks for a file named '.env' in the same folder and loads it.
load_dotenv() 

api_key = os.getenv("GEMINI_API_KEY")

# Check if the key was loaded successfully. If not, print an error and exit.
if not api_key:
    print("Error: The 'GEMINI_API_KEY' was not found.")
    print("Please make sure you have created a '.env' file with your key in it.")
    exit()

# Configure the Gemini library with your key.
genai.configure(api_key=api_key)


# --- 2. Set Up the Model ---
try:
    # We will use the 'gemini-1.5-flash-latest' model which is current and efficient.
    model = genai.GenerativeModel(model_name='gemini-1.5-flash-latest')

    # This is the detailed prompt you want to send to the AI.
    prompt = """
    Transforme o PDF ou Img (seja png ou jpeg) e transforme em planilha (sempre em formato markdown com tabelas e colunas separadas com o objetivo de apenas copiar para Excel).
    Leia tudo que está na imagem e coloque na planilha na seguinte ordem em colunas:
    Empresa (Nome da Empresa), Data (Com a Data Início e Data Fim, separe por -), Data Início, Data Fim, Campanha (Adicione o Nome da Campanha + Dia da Campanha que é o dia da oferta do encarte) + Estado (que é o estado do encarte), Categoria do Produto, Produto (Descrição, tire a referência do produto), Preço (Do Encarte), App (Preço para Usuários do App, se o Encarte falar), Cidade (Que mostrar no Encarte) e Estado (Que mostrar no Encarte, coloque somente a SIGLA DO ESTADO).
    Transforme uma de cada vez separando somente o que se extraiu na imagem separando em uma planilha. Sempre quando mandar novamente separe somente as informações da imagem que eu mandar.
    Sempre verifique duas vezes antes de fazer a leitura do encarte, verifique os erros antes da transformação.
    Em letras maiúsculas e minúsculas, respeitando a regra da língua portuguesa.
    """

    # --- 3. Run the Model and Print the Response ---
    print("Sending prompt to the Gemini API...")
    
    # Send the prompt to the model to generate the content.
    response = model.generate_content(prompt)

    # Print the model's text response to the console.
    print("\n--- Model Response ---")
    print(response.text)
    print("----------------------")

# Add general error handling for any other issues that might occur.
except Exception as e:
    print(f"An unexpected error occurred: {e}")
