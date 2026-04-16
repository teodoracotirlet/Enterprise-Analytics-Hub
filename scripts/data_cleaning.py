import pandas as pd
import os
import boto3
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError

# 1. Incarcam parolele din fisierul ascuns .env
load_dotenv()

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

print("Incepem procesarea datelor ERP...")

# 2. Generam si curatam datele (Logica de Business ramane la fel)
dirty_data = {
    'Client_ID': [101, 102, 103, 104, 105],
    'Nume_Companie': ['  auto romania srl ', 'TECH SOLUTIONS', None, 'qbs ReTail  ', 'fengmei automotive'],
    'Tara': ['ro', 'RO', 'Romania ', ' ro ', 'ROMANIA'],
    'Status_Plata': ['Platit', 'Intarziat', 'Platit', None, 'Intarziat']
}

df = pd.DataFrame(dirty_data)
df['Nume_Companie'] = df['Nume_Companie'].fillna('Necunoscut').str.strip().str.title()
df['Tara'] = df['Tara'].str.strip().str.upper().replace('ROMANIA', 'RO')
df['Status_Plata'] = df['Status_Plata'].fillna('Necunoscut')

# 3. Salvam fisierul local (temporar)
os.makedirs('data', exist_ok=True)
local_file_path = 'data/master_data_clienti.csv'
df.to_csv(local_file_path, index=False)
print(f"Date curatate si salvate local in: {local_file_path}")

# 4. Trimiterea datelor in AWS S3 (Data Lake)
print(f"Initiem conexiunea cu AWS S3 (Regiunea: {AWS_REGION})...")

try:
    # Cream "clientul" care vorbeste cu AWS
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    
    # Numele fisierului asa cum va aparea in Cloud
    s3_file_name = 'raw_data/master_data_clienti.csv'
    
    # Comanda de upload
    s3_client.upload_file(local_file_path, BUCKET_NAME, s3_file_name)
    print(f"✅ SUCCES! Fisierul a fost incarcat in S3 Bucket-ul '{BUCKET_NAME}' in folderul 'raw_data/'.")

except FileNotFoundError:
    print("Eroare: Fisierul local nu a fost gasit.")
except NoCredentialsError:
    print("Eroare: Credentialele AWS nu sunt corecte sau lipsesc din fisierul .env.")
except Exception as e:
    print(f"A aparut o eroare neasteptata: {e}")