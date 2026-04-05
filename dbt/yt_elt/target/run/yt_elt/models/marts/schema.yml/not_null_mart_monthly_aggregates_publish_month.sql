select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select publish_month
from "elt_db"."marts_marts"."mart_monthly_aggregates"
where publish_month is null



      
    ) dbt_internal_test