SELECT
    h.organisation_id AS organisation_id,
    h.processing_date AS processing_date,
    s.asset_class     AS asset_class,
    sum(h.market_value) AS total_aum,
    count()             AS positions
FROM silver.holdings AS h
INNER JOIN silver.securities AS s
    ON h.organisation_id = s.organisation_id
    AND h.security_id = s.security_id
    AND h.processing_date = s.processing_date
GROUP BY h.organisation_id, h.processing_date, s.asset_class
