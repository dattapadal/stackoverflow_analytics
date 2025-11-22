with questions_with_timing as (
    select
        fq.question_id,
        fq.created_date_key,
        fq.seconds_to_accepted_answer,
        d.year,
        d.quarter,
        d.month,
        d.year_month,
        d.month_name,
        
        case
            when fq.seconds_to_accepted_answer < 60 
                then '<1 min'
            when fq.seconds_to_accepted_answer >= 60 
                and fq.seconds_to_accepted_answer < 300 
                then '1-5 mins'
            when fq.seconds_to_accepted_answer >= 300 
                and fq.seconds_to_accepted_answer < 3600 
                then '5 mins-1 hour'
            when fq.seconds_to_accepted_answer >= 3600 
                and fq.seconds_to_accepted_answer < 10800 
                then '1-3 hours'
            when fq.seconds_to_accepted_answer >= 10800 
                and fq.seconds_to_accepted_answer < 86400 
                then '3 hours-1 day'
            when fq.seconds_to_accepted_answer >= 86400 
                and fq.seconds_to_accepted_answer < 604800 
                then '1-7 days'
            when fq.seconds_to_accepted_answer >= 604800 
                and fq.seconds_to_accepted_answer < 2592000 
                then '7-30 days'
            when fq.seconds_to_accepted_answer >= 2592000 
                then '>30 days'
            else 'No accepted answer'
        end as time_band,
        
        case
            when fq.seconds_to_accepted_answer < 60 then 1
            when fq.seconds_to_accepted_answer >= 60 and fq.seconds_to_accepted_answer < 300 then 2
            when fq.seconds_to_accepted_answer >= 300 and fq.seconds_to_accepted_answer < 3600 then 3
            when fq.seconds_to_accepted_answer >= 3600 and fq.seconds_to_accepted_answer < 10800 then 4
            when fq.seconds_to_accepted_answer >= 10800 and fq.seconds_to_accepted_answer < 86400 then 5
            when fq.seconds_to_accepted_answer >= 86400 and fq.seconds_to_accepted_answer < 604800 then 6
            when fq.seconds_to_accepted_answer >= 604800 and fq.seconds_to_accepted_answer < 2592000 then 7
            when fq.seconds_to_accepted_answer >= 2592000 then 8
            else 9
        end as time_band_order
        
    from fact_question fq
    inner join dim_date d
        on fq.created_date_key = d.date_key
    where fq.has_accepted_answer = true
),

time_series_by_band as (
    select
        year_month,
        year,
        month,
        month_name,
        time_band,
        time_band_order,
        count(distinct question_id) as question_count
        
    from questions_with_timing
    group by
        year_month,
        year,
        month,
        month_name,
        time_band,
        time_band_order
),

period_totals as (
    select
        year_month,
        sum(question_count) as total_questions_in_period
    from time_series_by_band
    group by year_month
),

-- Pivot the time bands into columns
pivoted_time_series as (
    select
        ts.year_month,
        ts.year,
        ts.month,
        ts.month_name,
        pt.total_questions_in_period,
        
        sum(case when ts.time_band = '<1 min' then ts.question_count else 0 end) as less_than_1_min,
        sum(case when ts.time_band = '1-5 mins' then ts.question_count else 0 end) as between_1_and_5_mins,
        sum(case when ts.time_band = '5 mins-1 hour' then ts.question_count else 0 end) as between_5_mins_and_1_hour,
        sum(case when ts.time_band = '1-3 hours' then ts.question_count else 0 end) as between_1_and_3_hours,
        sum(case when ts.time_band = '3 hours-1 day' then ts.question_count else 0 end) as between_3_hours_and_1_day,
        sum(case when ts.time_band = '1-7 days' then ts.question_count else 0 end) as between_1_and_7_days,
        sum(case when ts.time_band = '7-30 days' then ts.question_count else 0 end) as between_7_and_30_days,
        sum(case when ts.time_band = '>30 days' then ts.question_count else 0 end) as more_than_30_days
        
    from time_series_by_band ts
    inner join period_totals pt
        on ts.year_month = pt.year_month
    group by
        ts.year_month,
        ts.year,
        ts.month,
        ts.month_name,
        pt.total_questions_in_period
)

select
    year_month,
    year,
    month,
    month_name,
    total_questions_in_period,
    
    -- Time band columns
    less_than_1_min as "<1 min",
    between_1_and_5_mins as "1-5 mins",
    between_5_mins_and_1_hour as "5 mins-1 hour",
    between_1_and_3_hours as "1-3 hours",
    between_3_hours_and_1_day as "3 hours-1 day",
    between_1_and_7_days as "1-7 days",
    between_7_and_30_days as "7-30 days",
    more_than_30_days as ">30 days"
    
from pivoted_time_series
order by year_month