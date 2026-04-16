/*
  Model: Fact Sales Analytics
  Scop: Unirea tranzactiilor ERP curate cu Master Data-ul clientilor.
  Acesta este tabelul final (Data Mart) care va fi conectat direct in Power BI.
*/

WITH transactions AS (
    -- Folosim functia speciala dbt ref pentru a citi tabelul curatat la pasul anterior
    SELECT * FROM {{ ref('stg_erp_transactions') }}
),

clients AS (
    -- Preluam datele clientilor din schema RAW
    SELECT * FROM ENTERPRISE_HUB.RAW_DATA.MASTER_CLIENTI
)

SELECT
    t.TRANSACTION_ID,
    t.TRANSACTION_DATE,
    
    -- Aducem informatiile de business necesare pentru rapoarte
    c.NUME_COMPANIE AS CLIENT_NAME,
    c.TARA AS COUNTRY,
    
    t.PRODUCT_CODE,
    t.QUANTITY,
    t.UNIT_PRICE,
    t.TOTAL_TRANSACTION_VALUE,
    t.STATUS_PLATA AS PAYMENT_STATUS

FROM transactions t
LEFT JOIN clients c ON t.CLIENT_ID = c.CLIENT_ID