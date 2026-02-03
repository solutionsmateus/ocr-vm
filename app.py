import os
import glob
import zipfile
import time
from pathlib import Path
from dotenv import load_dotenv
from google import genai
# Importação corrigida: Adicionando GenerateContentConfig e SafetySetting
from google.genai.types import HarmCategory, HarmBlockThreshold, GenerateContentConfig, SafetySetting 
import pandas as pd
import io

# --- 1. Configuração Inicial ---
load_dotenv()
# Correção 1: Lendo a API key da variável de ambiente GEMINI_API_KEY (injeta pelo GitHub Secrets)
api_key = os.environ.get("GEMINI_API_KEY") 
artifact_folder = os.environ.get("ARTIFACT_FOLDER", "./workflow-github-action")


if not api_key:
    print("Erro: A 'GEMINI_API_KEY' não foi encontrada nas variáveis de ambiente.")
    print("Por favor, verifique se o secret está configurado e se o YAML o injeta corretamente (env: GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}).")
    exit()

client = genai.Client(api_key=api_key)

# Correção 3a: Definindo safety_settings como uma lista de objetos SafetySetting
safety_settings_list = [
    SafetySetting(
