
    
    

select
    publish_month as unique_field,
    count(*) as n_records

from "elt_db"."marts_marts"."mart_monthly_aggregates"
where publish_month is not null
group by publish_month
having count(*) > 1


