with videos as (

    select * from {{ ref('stg_videos') }}

),

engagement as (

    select
        video_id,
        title,
        published_at,
        view_count,
        like_count,
        comment_count,

        -- Engagement rate: (likes + comments) / views
        case
            when view_count = 0 then 0
            else round(
                (like_count + comment_count)::numeric / view_count::numeric * 100,
                4
            )
        end as engagement_rate_pct,

        -- Like rate: likes / views
        case
            when view_count = 0 then 0
            else round(
                like_count::numeric / view_count::numeric * 100,
                4
            )
        end as like_rate_pct,

        updated_at

    from videos

)

select * from engagement
order by engagement_rate_pct desc
