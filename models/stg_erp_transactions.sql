/*
  Acesta este un model de tip Staging (STG).
  Scopul lui este sa preia datele brute din ERP si sa aplice reguli stricte de Data Quality:
  1. Standardizarea codurilor de produs (BOM).
  2. Corectarea erorilor de introducere manuala (cantitati negative).
  3. Tratarea valorilor lipsa.
  4. Calcularea valorii totale a tranzactiei (Logica de Business).
*/

WITH raw_erp_data AS (
    -- Extragem datele din tabelul brut incarcat anterior
    SELECT * FROM ENTERPRISE_HUB.RAW_DATA.RAW_TRANSACTIONS
)

SELECT
    TRANSACTION_ID,
    CLIENT_ID,
    
    -- Standardizam codurile: scoatem spatiile si le facem majuscule (ex: 'bom-a100' devine 'BOM-A100')
    UPPER(TRIM(PRODUCT_CODE)) AS PRODUCT_CODE,
    
    -- Corectam cantitatile negative folosind valoarea absoluta (ABS)
    ABS(QUANTITY) AS QUANTITY,
    
    UNIT_PRICE,
    
    -- Aplicam logica financiara: calculam valoarea pe rand
    (ABS(QUANTITY) * UNIT_PRICE) AS TOTAL_TRANSACTION_VALUE,
    
    TRANSACTION_DATE,
    
    -- Inlocuim valorile NULL (lipsa) cu un status clar
    COALESCE(STATUS, 'Unknown') AS STATUS_PLATA

FROM raw_erp_data