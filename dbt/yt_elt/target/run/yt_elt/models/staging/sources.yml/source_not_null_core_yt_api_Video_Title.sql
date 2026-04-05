select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select Video_Title
from "elt_db"."core"."yt_api"
where Video_Title is null



      
    ) dbt_internal_test