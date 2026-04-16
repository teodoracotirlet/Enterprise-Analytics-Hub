import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os
import boto3
from dotenv import load_dotenv

# 1. Incarcam parolele pentru AWS
load_dotenv()
AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

print("Incepem generarea a 50.000 de tranzactii ERP...")

# 2. Generarea datelor sintetice
num_rows = 50000

# Setam niste ID-uri de clienti si coduri de produse (BOM)
client_ids = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110]
product_codes = ['BOM-A100', 'BOM-B200', 'BOM-C300', 'bom-a100', 'Bom-B200'] # Inconsecventa intentionata
statuses = ['Finalizat', 'In Procesare', 'Anulat', None] # Valori lipsa intentionate

# Generam date aleatoare pentru ultimele 365 de zile
start_date = datetime.now() - timedelta(days=365)
dates = [start_date + timedelta(days=random.randint(0, 365)) for _ in range(num_rows)]

data = {
    'TRANSACTION_ID': range(1, num_rows + 1),
    'CLIENT_ID': np.random.choice(client_ids, num_rows),
    'PRODUCT_CODE': np.random.choice(product_codes, num_rows),
    'QUANTITY': np.random.normal(50, 20, num_rows).astype(int), # Distributie normala
    'UNIT_PRICE': np.random.uniform(10.5, 500.0, num_rows).round(2),
    'TRANSACTION_DATE': dates,
    'STATUS': np.random.choice(statuses, num_rows, p=[0.7, 0.15, 0.05, 0.1]) # 10% sanse de NULL
}

df = pd.DataFrame(data)

# Injectam intentionat cateva cantitati negative (eroare umana in ERP)
df.loc[df.sample(frac=0.05).index, 'QUANTITY'] = -15 

print("Date generate cu succes! Previzualizare:")
print(df.head())

# 3. Salvarea locala
os.makedirs('data', exist_ok=True)
local_file_path = 'data/erp_transactions_50k.csv'
df.to_csv(local_file_path, index=False)
print(f"\nFisier salvat local: {local_file_path} (Dimensiune: {os.path.getsize(local_file_path) / (1024*1024):.2f} MB)")

# 4. Trimiterea in AWS S3 Data Lake
print(f"\nTrimitem fisierul catre AWS S3 in bucket-ul '{BUCKET_NAME}'...")
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    s3_file_name = 'raw_data/erp_transactions_50k.csv'
    s3_client.upload_file(local_file_path, BUCKET_NAME, s3_file_name)
    print("✅ SUCCES! Fisierul de 50.000 de randuri este acum in Data Lake.")
except Exception as e:
    print(f"Eroare la upload: {e}")