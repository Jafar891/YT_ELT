select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select view_rank
from "elt_db"."marts_marts"."mart_top_videos"
where view_rank is null



      
    ) dbt_internal_test