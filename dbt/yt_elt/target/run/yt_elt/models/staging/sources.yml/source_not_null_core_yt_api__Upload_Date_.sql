select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select "Upload_Date"
from "elt_db"."core"."yt_api"
where "Upload_Date" is null



      
    ) dbt_internal_test