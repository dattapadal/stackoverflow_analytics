with user_question_views as (
    select
        fq.user_key,
        u.display_name,
        u.reputation,
        u.reputation_tier,
        
        count(distinct fq.question_id) as total_questions_posted,
        sum(fq.view_count) as total_views_on_questions,
        
        -- Additional context
        sum(fq.question_score) as total_question_score,
        sum(case when fq.has_accepted_answer then 1 else 0 end) as questions_with_accepted_answers,
        sum(fq.actual_answer_count) as total_answers_received,
        
    from fact_question fq
    inner join dim_user u
        on fq.user_key = u.user_id
    where fq.view_count is not null
        and fq.view_count > 0
    group by
        fq.user_key,
        u.display_name,
        u.reputation,
        u.reputation_tier
),

ranked_users as (
    select
        *,
        row_number() over (order by total_views_on_questions desc) as view_rank
    from user_question_views
)

select
    view_rank,
    user_key as user_id,
    display_name,
    reputation,
    reputation_tier,
    total_questions_posted,
    total_views_on_questions,
    total_question_score,
    questions_with_accepted_answers,
    total_answers_received
from ranked_users
where view_rank <= 10
order by view_rank