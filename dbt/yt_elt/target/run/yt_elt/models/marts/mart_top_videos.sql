
  
    

  create  table "elt_db"."marts_marts"."mart_top_videos__dbt_tmp"
  
  
    as
  
  (
    with videos as (

    select * from "elt_db"."marts_staging"."stg_videos"

),

ranked as (

    select
        video_id,
        video_title,
        upload_date,
        video_views,
        likes_count,
        comments_count,
        rank() over (order by video_views desc) as view_rank
    from videos

)

select * from ranked
order by view_rank
  );
  