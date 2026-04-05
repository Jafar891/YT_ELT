select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select engagement_rate_pct
from "elt_db"."marts_marts"."mart_engagement_rate"
where engagement_rate_pct is null



      
    ) dbt_internal_test