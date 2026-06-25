WITH source AS (
    -- Notice how we use the source() macro instead of hardcoding MARKETPULSE_DB.BRONZE!
    SELECT * FROM {{ source('marketpulse_raw', 'RAW_HISTORICAL_OHLCV') }}
),

renamed_and_casted AS (
    SELECT 
        -- Cast the raw milliseconds into proper timestamps
        TO_TIMESTAMP_NTZ(DATE, 3) AS market_date,
        TICKER AS ticker,
        OPEN AS open_price,
        HIGH AS high_price,
        LOW AS low_price,
        CLOSE AS close_price,
        -- Handle null volumes
        COALESCE(VOLUME, 0) AS volume,
        INGESTED_AT AS bronze_ingested_at
    FROM source
    WHERE CLOSE IS NOT NULL
),

deduplicated AS (
    SELECT *
    FROM renamed_and_casted
    -- Keep only the most recent row if duplicates exist
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker, market_date ORDER BY bronze_ingested_at DESC) = 1
)

SELECT * FROM deduplicated