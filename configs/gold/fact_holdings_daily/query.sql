SELECT
    h.organisation_id AS organisation_id,
    h.processing_date AS processing_date,
    p.account_id      AS account_id,
    h.security_id     AS security_id,
    h.quantity        AS quantity,
    h.market_value    AS market_value
FROM silver.holdings AS h
INNER JOIN silver.portfolios_current AS p
    ON h.organisation_id = p.organisation_id AND h.portfolio_id = p.portfolio_id
