with source as (

    select * from "elt_db"."core"."yt_api"

),

renamed as (

    select
        "Video_ID"                        as video_id,
        "Video_Title"                     as video_title,
        "Upload_Date"                     as upload_date,
        "Duration"                        as duration,
        "Video_Type"                      as video_type,
        coalesce("Video_Views", 0)        as video_views,
        coalesce("Likes_Count", 0)        as likes_count,
        coalesce("Comments_Count", 0)     as comments_count
    from source

)

select * from renamed