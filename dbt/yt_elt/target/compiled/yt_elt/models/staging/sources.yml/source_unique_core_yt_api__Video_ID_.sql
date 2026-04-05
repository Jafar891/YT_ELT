
    
    

select
    "Video_ID" as unique_field,
    count(*) as n_records

from "elt_db"."core"."yt_api"
where "Video_ID" is not null
group by "Video_ID"
having count(*) > 1


