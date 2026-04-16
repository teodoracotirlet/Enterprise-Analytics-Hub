import pandas as pd
import os

print("Incepem procesarea datelor ERP...")

# 1. Generam date 'murdare' simulate (exact cum ar iesi dintr-un sistem vechi)
dirty_data = {
    'Client_ID': [101, 102, 103, 104, 105],
    'Nume_Companie': ['  auto romania srl ', 'TECH SOLUTIONS', None, 'qbs ReTail  ', 'fengmei automotive'],
    'Tara': ['ro', 'RO', 'Romania ', ' ro ', 'ROMANIA'],
    'Status_Plata': ['Platit', 'Intarziat', 'Platit', None, 'Intarziat']
}

# Transformam datele intr-un tabel (DataFrame)
df = pd.DataFrame(dirty_data)
print("\n--- Date Initiale (Murdare) ---")
print(df)

# 2. PROCESUL DE CURATARE (Data Cleaning)
print("\nCuratam datele...")

# A. Curatam Numele Companiei: eliminam spatiile inutile si punem litera mare la inceput
# Folosim fillna('Necunoscut') in caz ca lipseste numele
df['Nume_Companie'] = df['Nume_Companie'].fillna('Necunoscut').str.strip().str.title()

# B. Standardizam Tara: eliminam spatiile si facem totul majuscule (RO)
df['Tara'] = df['Tara'].str.strip().str.upper()
# Uniformizam: orice e 'ROMANIA' devine 'RO' pentru consistenta
df['Tara'] = df['Tara'].replace('ROMANIA', 'RO')

# C. Tratam valorile lipsa la Status Plata
df['Status_Plata'] = df['Status_Plata'].fillna('Necunoscut')

print("\n--- Date Finale (Curate) ---")
print(df)

# 3. Salvarea datelor in dosarul 'data'
# Ne asiguram ca dosarul exista (pentru siguranta)
os.makedirs('../data', exist_ok=True)

# Salvam fisierul CSV
output_path = 'data/master_data_clienti.csv'
df.to_csv(output_path, index=False)

print(f"\nSucces! Datele Master Data au fost salvate in: {output_path}")