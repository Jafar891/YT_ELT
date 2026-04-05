
  create view "elt_db"."marts_staging"."stg_videos__dbt_tmp"
    
    
  as (
    with source as (

    select * from "elt_db"."core"."videos"

),

renamed as (

    select
        video_id,
        title,
        published_at,
        coalesce(view_count, 0)    as view_count,
        coalesce(like_count, 0)    as like_count,
        coalesce(comment_count, 0) as comment_count,
        updated_at
    from source

)

select * from renamed
  );