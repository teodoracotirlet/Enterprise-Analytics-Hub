WITH raw_data AS (
    SELECT * FROM {{ source('erp_raw_data', 'RAW_TRANSACTIONS') }}
),

transformed AS (
    SELECT
        TRANSACTION_ID,
        SOURCE_SYSTEM,
        
        UPPER(TRIM(REGEXP_REPLACE(CUSTOMER_NAME, '[^a-zA-Z0-9 ]', ''))) AS CLEAN_CUSTOMER_NAME,

        CASE 
            WHEN SOURCE_SYSTEM = 'NovaTech_US' THEN TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY')
            WHEN SOURCE_SYSTEM = 'AeroForge_EU' AND ORDER_DATE LIKE '%-%' THEN TRY_TO_DATE(ORDER_DATE, 'DD-MM-YYYY')
            ELSE TO_DATE(TO_TIMESTAMP(ORDER_DATE::INT))
        END AS NORMALIZED_DATE,

        CASE 
            WHEN CURRENCY = 'USD' THEN RAW_AMOUNT * 0.92
            WHEN CURRENCY = 'GBP' THEN RAW_AMOUNT * 1.17
            ELSE RAW_AMOUNT 
        END AS AMOUNT_EUR,

        CURRENCY AS ORIGINAL_CURRENCY,
        RAW_AMOUNT AS ORIGINAL_AMOUNT,

        PRODUCT_METADATA:category::VARCHAR AS PRODUCT_CATEGORY,
        PRODUCT_METADATA:warranty_years::INT AS WARRANTY_PERIOD,
        PRODUCT_METADATA:qc_passed::BOOLEAN AS QUALITY_CHECK_STATUS

    FROM raw_data
    
    -- Aceasta este linia magica de deduplicare! 
    -- Snowflake va grupa randurile cu acelasi ID, le va ordona dupa data si il va pastra DOAR pe primul (1)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRANSACTION_ID ORDER BY NORMALIZED_DATE DESC) = 1
)

SELECT * FROM transformed