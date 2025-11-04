import requests
import csv
import os
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError

# --------------------------
# AWS Credentials
# --------------------------
AWS_ACCESS_KEY = "SUA_AWS_ACCESS_KEY_ID"
AWS_SECRET_KEY = "SUA_AWS_SECRET_ACCESS_KEY"
AWS_SESSION_TOKEN = "SUA_AWS_SESSION_TOKEN"  
BUCKET_NAME = "raw-felipeferro"
S3_PREFIX = "Crawler/Dados-Umidade"

# --------------------------
# Coordenadas de todos estados do Brasil (capitais)
# --------------------------
estados = {
    "AC": (-9.97, -67.81),
    "AL": (-9.66, -35.73),
    "AM": (-3.13, -60.02),
    "AP": (0.03, -51.07),
    "BA": (-12.97, -38.51),
    "CE": (-3.72, -38.54),
    "DF": (-15.83, -47.86),
    "ES": (-20.32, -40.34),
    "GO": (-16.68, -49.25),
    "MA": (-2.55, -44.30),
    "MG": (-19.92, -43.94),
    "MS": (-20.45, -54.62),
    "MT": (-15.60, -56.10),
    "PA": (-1.45, -48.49),
    "PB": (-7.12, -34.86),
    "PE": (-8.05, -34.90),
    "PI": (-5.09, -42.80),
    "PR": (-25.42, -49.27),
    "RJ": (-22.90, -43.17),
    "RN": (-5.80, -35.21),
    "RO": (-8.76, -63.90),
    "RR": (2.82, -60.67),
    "RS": (-30.03, -51.23),
    "SC": (-27.59, -48.55),
    "SE": (-10.91, -37.07),
    "SP": (-23.55, -46.63),
    "TO": (-10.25, -48.32)
}

# --------------------------
# Setup S3 Client
# --------------------------
s3 = boto3.client(
    's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    aws_session_token=AWS_SESSION_TOKEN
)

# --------------------------
# Função para baixar e salvar o CSV
# --------------------------
def baixar_umidade_estado(sigla, lat, lon):
    print(f"- Baixando dados de umidade para {sigla}...")

    inicio = "2025-01-01"
    fim = datetime.utcnow().strftime("%Y-%m-%d")

    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={inicio}&end_date={fim}"
        "&hourly=relative_humidity_2m"
        "&timezone=America/Sao_Paulo"
    )

    r = requests.get(url)

    if r.status_code != 200:
        print(f"- Erro ao buscar {sigla}: {r.status_code}")
        return None

    data = r.json()

    if "hourly" not in data or "time" not in data["hourly"]:
        print(f"- Dados incompletos para {sigla}")
        return None

    times = data["hourly"]["time"]
    humidities = data["hourly"]["relative_humidity_2m"]

    # arquivo final
    filename = f"umidade_{sigla}.csv"

    with open(filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["timestamp", "umidade"])

        for t, h in zip(times, humidities):
            # garante que só salva dados reais (sem previsões futuras)
            if t <= datetime.utcnow().strftime("%Y-%m-%dT%H:00"):
                writer.writerow([t, h])

    print(f"- CSV salvo: {filename}")
    return filename

# --------------------------
# Upload para S3
# --------------------------
def enviar_s3(filename):
    try:
        s3_path = f"{S3_PREFIX}/{filename}"
        s3.upload_file(filename, BUCKET_NAME, s3_path)
        print(f"- Enviado para S3: s3://{BUCKET_NAME}/{s3_path}")
    except NoCredentialsError:
        print("- Credenciais AWS inválidas ou ausentes")


# --------------------------
# Execução principal
# --------------------------
for sigla, coord in estados.items():
    file = baixar_umidade_estado(sigla, coord[0], coord[1])
    if file:
        enviar_s3(file)
