with videos as (

    select * from {{ ref('stg_videos') }}

),

monthly as (

    select
        date_trunc('month', published_at)::date  as publish_month,
        count(*)                                  as videos_published,
        sum(view_count)                           as total_views,
        sum(like_count)                           as total_likes,
        sum(comment_count)                        as total_comments,
        round(avg(view_count), 0)                 as avg_views_per_video,

        -- Average engagement rate for the month
        round(
            avg(
                case
                    when view_count = 0 then 0
                    else (like_count + comment_count)::numeric / view_count::numeric * 100
                end
            ),
            4
        ) as avg_engagement_rate_pct

    from videos
    group by 1

)

select * from monthly
order by publish_month desc
