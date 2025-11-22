with questions as (
    select * from {{ ref('int_questions') }}
),

answers as (
    select * from {{ ref('int_answers') }}
),

timing as (
    select * from {{ ref('int_question_answer_timing') }}
),

comments as (
    select * from {{ ref('stg_comments') }}
),

votes as (
    select * from {{ ref('stg_votes') }}
),

question_answer_stats as (
    select
        question_id,
        count(*) as answer_count,
        min(answer_created_at) as first_answer_at,
        max(answer_created_at) as last_answer_at,
        sum(score) as total_answer_score,
        avg(score) as avg_answer_score,
        max(score) as max_answer_score
    from answers
    group by question_id
),

question_comment_stats as (
    select
        q.question_id,
        count(*) as comment_count
    from questions q
    inner join comments c
        on q.question_id = c.post_id
    group by q.question_id
),

-- Vote statistics per question
question_vote_stats as (
    select
        q.question_id,
        count(*) as vote_count,
        sum(case when v.vote_type_id = 2 then 1 else 0 end) as upvote_count,
        sum(case when v.vote_type_id = 3 then 1 else 0 end) as downvote_count
    from questions q
    inner join votes v
        on q.question_id = v.post_id
    group by q.question_id
),

fact_question as (
    select
        q.question_id,
        
        q.owner_user_id as user_key,
        cast(q.question_created_at as date) as created_date_key,
        
        q.title,
        q.tags,
        
        q.score as question_score,
        q.view_count,
        q.answer_count as reported_answer_count,
        coalesce(a.answer_count, 0) as actual_answer_count,
        q.comment_count as reported_comment_count,
        coalesce(c.comment_count, 0) as actual_comment_count,
        q.favorite_count,
        
        a.total_answer_score,
        a.avg_answer_score,
        a.max_answer_score,
        
        coalesce(v.vote_count, 0) as vote_count,
        coalesce(v.upvote_count, 0) as upvote_count,
        coalesce(v.downvote_count, 0) as downvote_count,
        
        q.question_created_at,
        a.first_answer_at,
        a.last_answer_at,
        t.accepted_answer_created_at,
        t.seconds_to_accepted_answer,
        
        case 
            when a.first_answer_at is not null
            then {{ datediff('q.question_created_at', 'a.first_answer_at', 'second') }}
            else null
        end as seconds_to_first_answer,
        
        case 
            when q.accepted_answer_id is not null then true 
            else false 
        end as has_accepted_answer,
    
        q.question_last_edited_at,
        q.question_last_activity_at,
        
        current_localtimestamp() as _updated_at
        
    from questions q
    left join question_answer_stats a
        on q.question_id = a.question_id
    left join question_comment_stats c
        on q.question_id = c.question_id
    left join question_vote_stats v
        on q.question_id = v.question_id
    left join timing t
        on q.question_id = t.question_id
)

select * from fact_question