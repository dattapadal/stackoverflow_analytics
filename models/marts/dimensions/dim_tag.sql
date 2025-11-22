
with tags as (
    select * from {{ ref('stg_tags') }}
),

post_tags as (
    select * from {{ ref('int_post_tags') }}
),

tag_question_usage as (
    select
        tag_id,
        count(distinct case when post_type_id = 1 then post_id end) as question_count
    from post_tags
    group by tag_id
),

tag_dimension as (
    select
        t.tag_id,
        t.tag_name,
        t.count as total_usage_count,
        t.excerpt_post_id,
        t.wiki_post_id,
        
        coalesce(u.question_count, 0) as question_usage_count,
        
        case
            when t.count >= 100000 then 'Very Popular'
            when t.count >= 10000 then 'Popular'
            when t.count >= 1000 then 'Common'
            when t.count >= 100 then 'Occasional'
            else 'Rare'
        end as popularity_tier,
        
        current_localtimestamp() as _updated_at
        
    from tags t
    left join tag_question_usage u
        on t.tag_id = u.tag_id
)

select * from tag_dimension