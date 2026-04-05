with videos as (

    select * from {{ ref('stg_videos') }}

),

monthly as (

    select
        date_trunc('month', upload_date::timestamp)::date  as publish_month,
        count(*)                                            as videos_published,
        sum(video_views)                                    as total_views,
        sum(likes_count)                                    as total_likes,
        sum(comments_count)                                 as total_comments,
        round(avg(video_views), 0)                          as avg_views_per_video,
        round(
            avg(
                case
                    when video_views = 0 then 0
                    else (likes_count + comments_count)::numeric / video_views::numeric * 100
                end
            ),
            4
        ) as avg_engagement_rate_pct

    from videos
    group by 1

)

select * from monthly
order by publish_month desc