import os
from google import genai
import time

API_KEY = "AIzaSyB3YlLuIqoyV2JkJSISbVPvBHlfgSYlqts"
FILES = @artifacts

from google import genai
from google.genai import types

# Define the function declaration for the model
transform_ocr_xlsx = {
    "name": "transform_ocr_xlsx",
    "description": "Transform Images/PDFs to Spreadsheets.",
    "parameters": {
        "type": "object",
        "properties": {
            "attendees": {
                "type": "array",
                "items": {"type": "string"},
                "description": "List of Files Transform to XLSX",
            },
        },
        "required": ["attendees"],
    },
}

# Configure the client and tools
client = genai.Client()
tools = types.Tool(function_declarations=[transform_ocr_xlsx])
config = types.GenerateContentConfig(tools=[tools])

# Send request with function declarations
response = client.models.generate_content(
    model="gemini-2.5-flash",
    contents="Transforme o PDF ou Img (seja png ou jpeg) e transforme em planilha (sempre em formato markdown  com tabelas e colunas separadas com o objetivo de apenas copiar para Excel), transforme em xlsx, leia tudo que está na imagem e coloque na planilha na seguinte ordem em colunas: Empresa (Nome da Empresa), Data (Com a Data Início e Data Fim, separe por -), Data Início, Data Fim, Campanha (Adicione o Nome da Campanha + Dia da Campanha que é o dia da oferta do encarte) + Estado (que é o estado do encarte), Categoria do Produto,  Produto (Descrição, tire a referência do produto), Preço (Do Encarte), App (Preço para Usuários do App, se o Encarte falar), Cidade (Que mostrar no Encarte) e Estado (Que mostrar no Encarte, coloque somente a SIGLA DO ESTADO). Transforme uma de cada vez separando somente o que se extraiu na imagem separando em uma planilha, sempre quando mandar novamente separe somente as informações da imagem que eu mandar. Sempre verifique duas vezes antes de fazer a leitura do encarte, verifique os erros antes da transformação. Em letras maiúsculas e minúsculas, respeitando a regra da língua portuguesa.",
    config=config,
)

# Check for a function call
if response.candidates[0].content.parts[0].function_call:
    function_call = response.candidates[0].content.parts[0].function_call
    print(f"Function to call: {function_call.name}")
    print(f"Arguments: {function_call.args}")
    #  In a real app, you would call your function here:
    #  result = schedule_meeting(**function_call.args)
else:
    print("No function call found in the response.")
    print(response.text)


def get_answer():
    for i, file in enumerate(FILES):
        file = []
        rate = time.rate()
        output = file(FILES)
        #rate = time to process files
    return f"All files was process with sucessfull rate {output} {rate}"