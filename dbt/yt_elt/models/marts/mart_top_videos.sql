with videos as (

    select * from {{ ref('stg_videos') }}

),

ranked as (

    select
        video_id,
        title,
        published_at,
        view_count,
        like_count,
        comment_count,
        rank() over (order by view_count desc) as view_rank
    from videos

)

select * from ranked
order by view_rank
