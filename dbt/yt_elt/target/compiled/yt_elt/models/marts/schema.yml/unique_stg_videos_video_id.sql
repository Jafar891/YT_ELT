
    
    

select
    video_id as unique_field,
    count(*) as n_records

from "elt_db"."marts_staging"."stg_videos"
where video_id is not null
group by video_id
having count(*) > 1


