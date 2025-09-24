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
)
select 
    lp.visit_date,
    count(distinct lp.visitor_id) as visitors_count,
    lp.utm_source,
    lp.utm_medium,
    lp.utm_campaign,
    coalesce(sum(ya.daily_spent),0) + coalesce(sum(vk.daily_spent),0) as total_cost,
    count(distinct case when l.lead_id is not null then l.visitor_id end) as leads_count,
    count(case when l.closing_reason = 'Успешно реализовано' or l.status_id = 142 then l.lead_id end) as purchases_count,
    sum(case when l.closing_reason = 'Успешно реализовано' or l.status_id = 142 then l.amount else 0 end) as revenue  
from 
    last_paid_visit as lp
inner join 
    ya_ads as ya on lp.utm_source = ya.utm_source
left join 
    vk_ads as vk on lp.utm_source = vk.utm_source
left join 
    leads as l on lp.visitor_id = l.visitor_id and lp.visit_date <= l.created_at
where 
    rn = 1
group by 
    lp.visit_date, 
    lp.utm_source, 
    lp.utm_medium, 
    lp.utm_campaign
order by 
    lp.visit_date,
    visitors_count desc,
    lp.utm_source, 
    lp.utm_medium, 
    lp.utm_campaign,
    revenue desc nulls last;