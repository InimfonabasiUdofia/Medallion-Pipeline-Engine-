SELECT
    organisation_id,
    processing_date,
    side,
    countState()                  AS trades_state,
    uniqState(portfolio_id)       AS portfolios_state,
    sumState(quantity * price)    AS notional_state
FROM silver.trades
GROUP BY organisation_id, processing_date, side
