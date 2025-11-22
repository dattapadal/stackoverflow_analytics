with comment_mentions as (
    select * from int_comment_mentions
),

-- Questions with @mentions by the question owner
questions_with_mentions as (
    select
        cm.question_id,
        count(distinct cm.comment_id) as mention_count,
        count(distinct case when cm.comment_on_type = 'question' then cm.comment_id end) as mentions_on_question,
        count(distinct case when cm.comment_on_type = 'answer' then cm.comment_id end) as mentions_on_answers
    from comment_mentions cm
    group by cm.question_id
),

-- Summary statistics
question_mention_analysis as (
    select
        fq.question_id,
        fq.user_key,
        u.display_name as question_owner,
        u.reputation,
        fq.title,
        fq.question_created_at,
        fq.view_count,
        fq.question_score,
        fq.actual_answer_count,
        fq.has_accepted_answer,
        
        coalesce(qm.mention_count, 0) as total_mentions_by_owner,
        coalesce(qm.mentions_on_question, 0) as mentions_on_question,
        coalesce(qm.mentions_on_answers, 0) as mentions_on_answers,

        case 
            when qm.mention_count > 0 then true 
            else false 
        end as has_owner_mentions,
        
    from fact_question fq
    inner join dim_user u
        on fq.user_key = u.user_id
    left join questions_with_mentions qm
        on fq.question_id = qm.question_id
)

select
    count(distinct question_id) as total_questions_with_owner_mentions,
    sum(total_mentions_by_owner) as total_mention_count,
    sum(mentions_on_question) as total_mentions_on_questions,
    sum(mentions_on_answers) as total_mentions_on_answers
    
from question_mention_analysis
where has_owner_mentions = true
order by total_mention_count desc
