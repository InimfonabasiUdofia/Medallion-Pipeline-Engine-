SELECT
    a.organisation_id AS organisation_id,
    a.account_id      AS account_id,
    a.household_id    AS household_id,
    a.account_type    AS account_type,
    a.status          AS status,
    a.opened_date     AS opened_date,
    c.name            AS custodian_name
FROM silver.accounts_current AS a
LEFT JOIN silver.custodians_current AS c
    ON a.organisation_id = c.organisation_id AND a.custodian_id = c.custodian_id
