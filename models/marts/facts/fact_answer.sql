
with answers as (
    select * from {{ ref('int_answers') }}
),

questions as (
    select * from {{ ref('int_questions') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

votes as (
    select * from {{ ref('stg_votes') }}
),

answer_comment_stats as (
    select
        a.answer_id,
        count(*) as comment_count
    from answers a
    inner join comments c
        on a.answer_id = c.post_id
    group by a.answer_id
),

answer_vote_stats as (
    select
        a.answer_id,
        count(*) as vote_count,
        sum(case when v.vote_type_id = 2 then 1 else 0 end) as upvote_count,
        sum(case when v.vote_type_id = 3 then 1 else 0 end) as downvote_count
    from answers a
    inner join votes v
        on a.answer_id = v.post_id
    group by a.answer_id
),

fact_answer as (
    select
        a.answer_id,
        a.question_id,
        
        a.owner_user_id as user_key,
        q.owner_user_id as question_owner_user_key,
        cast(a.answer_created_at as date) as created_date_key,
        
        a.score as answer_score,
        a.comment_count as reported_comment_count,
        coalesce(c.comment_count, 0) as actual_comment_count,
        
        coalesce(v.vote_count, 0) as vote_count,
        coalesce(v.upvote_count, 0) as upvote_count,
        coalesce(v.downvote_count, 0) as downvote_count,
        
        a.answer_created_at,
        q.question_created_at,
        {{ datediff('q.question_created_at', 'a.answer_created_at', 'second') }} as seconds_after_question,
        
        case
            when seconds_after_question < 60 then 'Within 1 minute'
            when seconds_after_question < 300 then 'Within 5 minutes'
            when seconds_after_question < 3600 then 'Within 1 hour'
            when seconds_after_question < 86400 then 'Within 1 day'
            when seconds_after_question < 604800 then 'Within 1 week'
            else 'After 1 week'
        end as answer_timing_band,
        
        case 
            when q.accepted_answer_id = a.answer_id then true 
            else false 
        end as is_accepted_answer,
        
        case 
            when a.owner_user_id = q.owner_user_id then true 
            else false 
        end as is_self_answer,
        
        a.answer_last_edit_at,
        
        current_localtimestamp() as _updated_at
        
    from answers a
    inner join questions q
        on a.question_id = q.question_id
    left join answer_comment_stats c
        on a.answer_id = c.answer_id
    left join answer_vote_stats v
        on a.answer_id = v.answer_id
)

select * from fact_answer