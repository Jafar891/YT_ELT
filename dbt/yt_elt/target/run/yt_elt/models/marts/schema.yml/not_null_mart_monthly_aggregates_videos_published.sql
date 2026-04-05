select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select videos_published
from "elt_db"."marts_marts"."mart_monthly_aggregates"
where videos_published is null



      
    ) dbt_internal_test