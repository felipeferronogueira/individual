import csv
import os
from datetime import datetime, timedelta # <== Importação de timedelta adicionada
import boto3
from botocore.exceptions import NoCredentialsError
import urllib.request
import json

# =========================================================================
# CONFIGURAÇÕES AWS
# =========================================================================
BUCKET_NAME = "raw-felipeferro"
S3_PREFIX = "Crawler/Dados-Umidade"
s3 = boto3.client("s3")

# =========================================================================
# DADOS ESPECÍFICOS: APENAS SÃO PAULO (SP)
# Coordenadas: São Paulo (-23.55, -46.63)
# =========================================================================
estados_sp = {
    "SP": (-23.55, -46.63)
}

def baixar_umidade_estado(sigla, lat, lon):
    """
    Busca dados históricos de umidade em um local específico usando Open-Meteo.
    Salva os dados em um arquivo CSV temporário.
    """
    print(f"-> Baixando dados de umidade para {sigla}...")

    # Data de início da coleta (pode ser ajustada)
    inicio = "2025-01-01"
    # Data de fim: agora (UTC) formatada para o padrão da API
    fim = datetime.utcnow().strftime("%Y-%m-%d")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={inicio}&end_date={fim}"
        "&hourly=relative_humidity_2m"
        "&timezone=America/Sao_Paulo" # Fuso horário brasileiro na API
    )
    
    # ⚠️ Importante: Para funções Lambda, é bom usar try/except para requisições externas
    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
    except Exception as e:
        print(f"ERRO ao acessar a API para {sigla}: {e}")
        return None

    if "hourly" not in data or "time" not in data["hourly"]:
        print(f"-> Dados incompletos ou formato inesperado para {sigla}")
        return None

    times = data["hourly"]["time"]
    humidities = data["hourly"]["relative_humidity_2m"]

    filename = f"/tmp/umidade_{sigla}.csv"

    # =================================================================
    # CORREÇÃO DO FUSO HORÁRIO AQUI:
    # 1. Obtém o horário UTC da Lambda.
    # 2. Subtrai 3 horas (diferença UTC para GMT-3 São Paulo).
    sao_paulo_time = datetime.utcnow() - timedelta(hours=3)
    
    # 3. Formata o horário de São Paulo para usar no filtro.
    current_time_sp_str = sao_paulo_time.strftime("%Y-%m-%dT%H:00")
    print(f"-> Horário de referência para filtro (SP): {current_time_sp_str}")
    # =================================================================
    
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "umidade_relativa_percentual"])
        for t, h in zip(times, humidities):
            # Filtra apenas dados que já ocorreram (timestamp < hora atual de SP)
            if t < current_time_sp_str: 
                writer.writerow([t, h])

    print(f"-> CSV salvo localmente: {filename}")
    return filename

def enviar_s3(filename):
    """
    Envia o arquivo CSV temporário para o bucket S3.
    """
    try:
        s3_key = f"{S3_PREFIX}/{os.path.basename(filename)}"
        s3.upload_file(filename, BUCKET_NAME, s3_key)
        print(f"-> Enviado: s3://{BUCKET_NAME}/{s3_key}")

    except NoCredentialsError:
        print("ERRO S3: Lambda sem credenciais (verifique a role de execução)")
    except Exception as e:
        print(f"ERRO S3: Falha ao enviar para o S3: {e}")

# =========================================================================
# HANDLER PRINCIPAL DO LAMBDA
# =========================================================================
def lambda_handler(event, context):
    print("Iniciando coleta de umidade de SP no Lambda")

    # Apenas itera sobre o dicionário 'estados_sp'
    for sigla, coord in estados_sp.items():
        file = baixar_umidade_estado(sigla, coord[0], coord[1])
        if file:
            enviar_s3(file)
            
    # Limpeza opcional (liberar espaço /tmp)
    for sigla in estados_sp.keys():
        temp_file = f"/tmp/umidade_{sigla}.csv"
        if os.path.exists(temp_file):
            os.remove(temp_file)
            print(f"-> Arquivo temporário removido: {temp_file}")


    print("Finalizado com sucesso")
    return {"status": "OK", "mensagem": "Coleta de umidade de SP concluída"}
