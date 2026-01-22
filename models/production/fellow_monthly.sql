{{ config(materialized='table', schema='production') }}

with base as (
    select *
    from {{ ref('ltm_studentlevel') }}
),
typed as (
    select
        programme_name,
        monthyear,
        center_id,
        student_id,
        total_sessions,
        case
            when fellow_id is not null then 'fellow'
            when pair_id is not null then 'pair'
            else 'unknown'
        end as facilitator_type,
        coalesce(fellow_id, pair_id) as facilitator_id,
        case
            when fellow_id is not null then 1
            when pair_id is not null then 2
            else 0
        end as fellow_count
    from base
),
centre_month_dedup as (
    select
        programme_name,
        monthyear,
        facilitator_type,
        facilitator_id,
        fellow_count,
        center_id,
        max(total_sessions) as centre_total_sessions
    from typed
    where facilitator_id is not null
    group by
        programme_name,
        monthyear,
        facilitator_type,
        facilitator_id,
        fellow_count,
        center_id
)

select
    programme_name,
    monthyear,
    facilitator_type,
    facilitator_id,
    fellow_count,
    count(distinct center_id) as centre_count,
    sum(centre_total_sessions) as sessions_done
from centre_month_dedup
group by
    programme_name,
    monthyear,
    facilitator_type,
    facilitator_id,
    fellow_count
