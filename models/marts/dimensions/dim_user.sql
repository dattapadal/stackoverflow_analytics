
with users as (
    select * from {{ ref('stg_users') }}
),

badges as (
    select * from {{ ref('stg_badges') }}
),

posts as (
    select * from {{ ref('stg_posts') }}
),

user_badges as (
    select
        user_id,
        count(*) as total_badges,
        sum(case when class = 1 then 1 else 0 end) as gold_badges,
        sum(case when class = 2 then 1 else 0 end) as silver_badges,
        sum(case when class = 3 then 1 else 0 end) as bronze_badges
    from badges
    group by user_id
),

user_posts as (
    select
        owner_user_id as user_id,
        count(*) as total_posts,
        sum(case when post_type_id = 1 then 1 else 0 end) as question_count,
        sum(case when post_type_id = 2 then 1 else 0 end) as answer_count,
        sum(score) as total_post_score,
        min(creation_date) as first_post_date,
        max(creation_date) as last_post_date
    from posts
    where owner_user_id is not null
    group by owner_user_id
),

-- Build user dimension
user_dimension as (
    select
        u.user_id,
        u.display_name,
        u.about_me,
        u.reputation,
        u.views as profile_views,
        u.upvotes,
        u.downvotes,
        
        coalesce(u.upvotes - u.downvotes, 0) as net_votes,
        case 
            when u.upvotes + u.downvotes > 0 
            then round(cast(u.upvotes as numeric) / (u.upvotes + u.downvotes), 3)
            else null 
        end as upvote_ratio,
        
        coalesce(b.total_badges, 0) as total_badges,
        coalesce(b.gold_badges, 0) as gold_badges,
        coalesce(b.silver_badges, 0) as silver_badges,
        coalesce(b.bronze_badges, 0) as bronze_badges,
        
        coalesce(p.total_posts, 0) as total_posts,
        coalesce(p.question_count, 0) as question_count,
        coalesce(p.answer_count, 0) as answer_count,
        coalesce(p.total_post_score, 0) as total_post_score,
        p.first_post_date,
        p.last_post_date,
        
        u.creation_date as user_created_at,
        u.last_access_date as last_accessed_at,
        {{ datediff('u.creation_date', 'current_localtimestamp()', 'day') }} as days_since_joined,
        
        case
            when u.reputation >= 25000 then 'Elite'
            when u.reputation >= 10000 then 'Expert'
            when u.reputation >= 3000 then 'Established'
            when u.reputation >= 500 then 'Intermediate'
            when u.reputation >= 100 then 'Beginner'
            else 'New'
        end as reputation_tier,
        
        current_localtimestamp() as _updated_at
        
    from users u
    left join user_badges b
        on u.user_id = b.user_id
    left join user_posts p
        on u.user_id = p.user_id
)

select * from user_dimension