
with questions as (
    select * from {{ ref('int_questions') }}
),

answers as (
    select * from {{ ref('int_answers') }}
),

question_accepted_answer as (
    select
        q.question_id,
        q.owner_user_id as question_owner_id,
        q.question_created_at,
        a.answer_id as accepted_answer_id,
        a.answer_created_at as accepted_answer_created_at,
        a.owner_user_id as answer_owner_id,
        {{ datediff('q.question_created_at', 'a.answer_created_at', 'second') }} as seconds_to_accepted_answer
        
    from questions q
    inner join answers a
        on q.accepted_answer_id = a.answer_id
    where q.accepted_answer_id is not null
)

select * from question_accepted_answer