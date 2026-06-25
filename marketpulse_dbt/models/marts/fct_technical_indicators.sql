WITH staging AS (
    -- Reference our clean Silver model, not the raw table!
    SELECT * FROM {{ ref('stg_historical_ohlcv') }}
),

price_changes AS (
    -- Calculate the daily gain or loss for RSI
    SELECT
        *,
        close_price - LAG(close_price) OVER (PARTITION BY ticker ORDER BY market_date) AS daily_change
    FROM staging
),

rolling_metrics AS (
    SELECT
        *,
        -- Bollinger Bands components
        AVG(close_price) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS sma_20,
        STDDEV(close_price) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS stddev_20,

        -- MACD components (Using 12-day and 26-day averages)
        AVG(close_price) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) AS macd_fast_12,
        AVG(close_price) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 25 PRECEDING AND CURRENT ROW) AS macd_slow_26,

        -- RSI components (Separate gains and losses)
        CASE WHEN daily_change > 0 THEN daily_change ELSE 0 END AS gain,
        CASE WHEN daily_change < 0 THEN ABS(daily_change) ELSE 0 END AS loss
    FROM price_changes
),

rsi_averages AS (
    -- Average the gains and losses over 14 days
    SELECT
        *,
        AVG(gain) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_gain_14,
        AVG(loss) OVER (PARTITION BY ticker ORDER BY market_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS avg_loss_14
    FROM rolling_metrics
)

-- Assemble the Final Gold Table
SELECT
    market_date,
    ticker,
    close_price,
    volume,

    -- 1. Simple Moving Average
    sma_20,

    -- 2. Bollinger Bands
    sma_20 + (2 * stddev_20) AS bollinger_upper_band,
    sma_20 - (2 * stddev_20) AS bollinger_lower_band,

    -- 3. MACD Line
    (macd_fast_12 - macd_slow_26) AS macd_line,

    -- 4. RSI (0 to 100 Oscillator)
    CASE
        WHEN avg_loss_14 = 0 THEN 100
        ELSE 100 - (100 / (1 + (avg_gain_14 / NULLIF(avg_loss_14, 0))))
    END AS rsi_14

FROM rsi_averages
ORDER BY ticker, market_date DESC