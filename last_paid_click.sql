WITH last_paid_visit AS (
    SELECT
        distinct s.visitor_id,
        s.visit_date,
        s.source,
        s.medium,
        s.campaign,
        ROW_NUMBER()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
            AS rn
    FROM
        sessions AS s
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
)
SELECT
    lp.visitor_id,
    lp.visit_date,
    lp.source as utm_source,
    lp.medium as utm_medium,
    lp.campaign as utm_campaign,
    l.lead_id,
    l.created_at,
    l.amount,
    l.closing_reason,
    l.status_id
FROM
    last_paid_visit AS lp
LEFT JOIN
    leads AS l
    ON lp.visitor_id = l.visitor_id
    AND lp.visit_date <= l.created_at
WHERE
    lp.rn = 1
ORDER BY
    amount DESC NULLS LAST,
    lp.visit_date ASC,
    utm_source ASC,
    utm_medium ASC,
    utm_campaign asc;