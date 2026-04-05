select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

select
    video_id as unique_field,
    count(*) as n_records

from "elt_db"."marts_marts"."mart_top_videos"
where video_id is not null
group by video_id
having count(*) > 1



      
    ) dbt_internal_test