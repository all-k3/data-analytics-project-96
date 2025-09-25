WITH last_paid_visit AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        ROW_NUMBER()
            OVER (PARTITION BY s.visitor_id ORDER BY s.visit_date DESC)
            AS rn
    FROM
        sessions AS s
    WHERE
        LOWER(s.medium) IN (
            'cpc', 'cpm', 'cpa', 'youtube', 'cpp', 'tg', 'social'
        )
),

combined_ads AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        campaign_date,
        SUM(daily_spent) AS total_cost
    FROM (
        SELECT
            utm_source,
            utm_medium,
            utm_campaign,
            campaign_date,
            daily_spent
        FROM ya_ads
        UNION ALL
        SELECT
            utm_source,
            utm_medium,
            utm_campaign,
            campaign_date,
            daily_spent
        FROM vk_ads
    ) AS combined
    GROUP BY utm_source, utm_medium, utm_campaign, campaign_date
)

SELECT
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign,
    DATE(lp.visit_date) AS visit_date,
    COUNT(lp.visitor_id) AS visitors_count,
    COALESCE(SUM(ca.total_cost), 0) AS total_cost,
    COUNT(DISTINCT l.visitor_id) AS leads_count,
    COUNT(
        CASE
            WHEN
                l.closing_reason = 'Успешно реализовано' OR l.status_id = 142
                THEN l.lead_id
        END
    ) AS purchases_count,
    SUM(
        CASE
            WHEN
                l.closing_reason = 'Успешно реализовано' OR l.status_id = 142
                THEN l.amount
            ELSE 0
        END
    ) AS revenue
FROM
    last_paid_visit AS lp
LEFT JOIN
    combined_ads AS ca
    ON
        lp.utm_source = ca.utm_source
        AND lp.utm_medium = ca.utm_medium
        AND lp.utm_campaign = ca.utm_campaign
        AND DATE(lp.visit_date) = DATE(ca.campaign_date)
LEFT JOIN
    leads AS l
    ON
        lp.visitor_id = l.visitor_id
        AND DATE(lp.visit_date) <= DATE(l.created_at)
WHERE
    lp.rn = 1
GROUP BY
    DATE(lp.visit_date),
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign
ORDER BY
    visit_date ASC,
    visitors_count DESC,
    lp.utm_source ASC,
    lp.utm_medium ASC,
    lp.utm_campaign ASC,
    revenue DESC NULLS LAST;
