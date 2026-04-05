select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select view_count
from "elt_db"."marts_staging"."stg_videos"
where view_count is null



      
    ) dbt_internal_test