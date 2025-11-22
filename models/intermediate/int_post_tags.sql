with posts as (
    select * from {{ ref('stg_posts') }}
),

tags as (
    select * from {{ ref('stg_tags') }}
),

posts_with_tags as (
    select
        post_id,
        post_type_id,
        tags,
        replace(replace(tags, '<', ''), '>', ',') as parsed_tags
    from posts
    where tags is not null
        and tags != ''
),
numbers as (
    select range as n 
    from range(1,11)
),
post_tag_pairs as (
    select
        post_id,
        post_type_id,
        trim(regexp_replace(tag, '[<>]', '', '')) as tag_name
    from posts_with_tags,
    unnest(string_split(tags, '><')) as t(tag)
    where trim(regexp_replace(tag, '[<>]', '', '')) != ''
)

select
    p.post_id,
    p.post_type_id,
    p.tag_name,
    t.tag_id,
    t.count
from post_tag_pairs p
left join tags t
    on lower(p.tag_name) = lower(t.tag_name)