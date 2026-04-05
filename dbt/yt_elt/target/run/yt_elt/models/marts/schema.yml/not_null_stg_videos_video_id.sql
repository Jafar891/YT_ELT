select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select video_id
from "elt_db"."marts_staging"."stg_videos"
where video_id is null



      
    ) dbt_internal_test