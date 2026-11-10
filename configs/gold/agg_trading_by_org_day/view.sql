SELECT
    organisation_id,
    processing_date,
    side,
    countMerge(trades_state)       AS trades,
    uniqMerge(portfolios_state)    AS active_portfolios,
    sumMerge(notional_state)       AS notional
FROM gold.agg_trading_by_org_day
GROUP BY organisation_id, processing_date, side
