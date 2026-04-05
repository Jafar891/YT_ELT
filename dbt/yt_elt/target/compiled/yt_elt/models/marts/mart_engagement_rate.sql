with videos as (

    select * from "elt_db"."marts_staging"."stg_videos"

),

engagement as (

    select
        video_id,
        video_title,
        upload_date,
        video_views,
        likes_count,
        comments_count,

        case
            when video_views = 0 then 0
            else round(
                (likes_count + comments_count)::numeric / video_views::numeric * 100,
                4
            )
        end as engagement_rate_pct,

        case
            when video_views = 0 then 0
            else round(
                likes_count::numeric / video_views::numeric * 100,
                4
            )
        end as like_rate_pct

    from videos

)

select * from engagement
order by engagement_rate_pct desc