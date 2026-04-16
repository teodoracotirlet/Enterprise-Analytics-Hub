/*
  Model: Fact Global Sales
  Scop: Tabelul final de tip Data Mart pregatit pentru Power BI / Tableau.
  Logica de business aplicata:
  1. Extragerea lunii si anului pentru rapoarte de trend.
  2. Calculul estimativ al costurilor si profitului pe baza categoriei de produs.
  3. Segmentarea automata a clientilor (VIP vs. Small Business).
*/

WITH staging_data AS (
    -- Referentiem modelul anterior pe care tocmai l-am curatat si testat
    SELECT * FROM {{ ref('stg_global_transactions') }}
),

final_fact AS (
    SELECT
        TRANSACTION_ID,
        NORMALIZED_DATE AS TRANSACTION_DATE,
        
        -- Extragem Luna si Anul pentru vizualizari usoare in dashboard-uri
        TO_CHAR(NORMALIZED_DATE, 'YYYY-MM') AS SALES_MONTH,
        
        SOURCE_SYSTEM,
        CLEAN_CUSTOMER_NAME AS CUSTOMER_NAME,
        PRODUCT_CATEGORY,
        WARRANTY_PERIOD,
        QUALITY_CHECK_STATUS,
        AMOUNT_EUR AS REVENUE_EUR,

        -- Logica de Business 1: Calculam un profit estimativ 
        -- (Ex: 30% marja la Turbine, 40% la restul produselor)
        CASE 
            WHEN PRODUCT_CATEGORY = 'Turbines' THEN AMOUNT_EUR * 0.30
            ELSE AMOUNT_EUR * 0.40
        END AS ESTIMATED_PROFIT_EUR,

        -- Logica de Business 2: Categorisirea automata a tranzactiilor
        CASE 
            WHEN AMOUNT_EUR >= 7500 THEN 'Enterprise / VIP'
            WHEN AMOUNT_EUR >= 2500 THEN 'Mid-Market'
            ELSE 'Small Business'
        END AS CUSTOMER_SEGMENT

    FROM staging_data
    -- Ne asiguram ca tranzactiile fara data valida nu intra in raportul financiar
    WHERE NORMALIZED_DATE IS NOT NULL 
)

SELECT * FROM final_fact