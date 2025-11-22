
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2001-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

date_dimension as (
    select
        cast(date_day as date) as date_key,
        date_day as full_date,
        
        extract(year     from date_day) as year,
        extract(quarter  from date_day) as quarter,
        extract(month    from date_day) as month,
        extract(week     from date_day) as week_of_year,
        extract(day      from date_day) as day_of_month,
        extract(dow      from date_day) as day_of_week,     
        extract(doy      from date_day) as day_of_year,
        
        strftime(date_day, '%B')    as month_name,
        strftime(date_day, '%b')    as month_name_short,
        strftime(date_day, '%A')    as day_name,
        strftime(date_day, '%a')    as day_name_short,
        
        extract(year from date_day)     as fiscal_year,
        extract(quarter from date_day)  as fiscal_quarter,
        
        case 
            when extract(dow from date_day) in (0, 6) then true  
            else false
        end as is_weekend,
        
        case 
            when extract(day from date_day) = 1 then true 
            else false 
        end as is_month_start,
        
        case 
            when extract(day from last_day(date_day)) = extract(day from date_day)
                then true 
            else false 
        end as is_month_end,
        
        strftime(date_day, '%Y-%m') as year_month
        
    from date_spine
)

select * from date_dimension
