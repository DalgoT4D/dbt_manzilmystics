{{ config(materialized='table', schema='production') }}

with base as (
    select *
    from {{ ref('ltm_studentlevel') }}
),
numbered as (
    select
        *,
        case
            when pre_assessment is null then null
            when lower(trim(pre_assessment::text)) in ('true', 't', 'yes', 'y', '1') then true
            when lower(trim(pre_assessment::text)) in ('false', 'f', 'no', 'n', '0') then false
            else null
        end as pre_assessment_bool,
        row_number() over (
            partition by programme_name, center_id, monthyear
            order by student_id
        ) as student_row_number
    from base
)

select
    programme_name,
    center_id,
    monthyear,

    count(distinct student_id) as student_count,
    (count(distinct fellow_id) + (2 * count(distinct pair_id))) as fellow_count,

    coalesce(
        max(case when student_row_number = 1 then total_sessions end),
        max(total_sessions)
    ) as total_sessions,
    -- sum(coalesce(sessions_present, 0)) as sessions_present_total,
    -- sum(coalesce(sessions_absent, 0)) as sessions_absent_total,
    -- sum(coalesce(sessions_present, 0) + coalesce(sessions_absent, 0)) as attendance_denominator,
    round(sum(coalesce(sessions_present, 0))::numeric
    / nullif(sum(coalesce(sessions_present, 0) + coalesce(sessions_absent, 0)), 0),2)*100 as attendance_pct,

    round(
        sum(case when pre_assessment_bool is true then 1 else 0 end)::numeric
        / nullif(sum(case when pre_assessment_bool is not null then 1 else 0 end), 0),
        2
    ) * 100 as pre_assessment_complete_pct,

    sum(total_marks) as total_marks_sum,
    round(avg(total_marks),0) as total_marks_avg,
    sum(emotionscore) as emotionscore_sum,
    round(avg(emotionscore),0) as emotionscore_avg,
    sum(creativityscore) as creativityscore_sum,
    round(avg(creativityscore),0) as creativityscore_avg,
    sum(interactionscore) as interactionscore_sum,
    round(avg(interactionscore),0) as interactionscore_avg,
    sum(involvementscore) as involvementscore_sum,
    round(avg(involvementscore),0) as involvementscore_avg,

    sum(total_score_50) as total_score_50_sum,
    round(avg(total_score_50),0) as total_score_50_avg,
    sum(holdonlaya_score) as holdonlaya_score_sum,
    round(avg(holdonlaya_score),0) as holdonlaya_score_avg,
    sum(attendance_score) as attendance_score_sum,
    round(avg(attendance_score),0) as attendance_score_avg,
    sum(effort_homework_memorize_score) as effort_homework_memorize_score_sum,
    round(avg(effort_homework_memorize_score),0) as effort_homework_memorize_score_avg,
    sum(readinesstograd_score) as readinesstograd_score_sum,
    round(avg(readinesstograd_score),0) as readinesstograd_score_avg,
    sum(engagementwhatsapp_score) as engagementwhatsapp_score_sum,
    round(avg(engagementwhatsapp_score),0) as engagementwhatsapp_score_avg,
    sum(performance_readiness_score) as performance_readiness_score_sum,
    round(avg(performance_readiness_score),0) as performance_readiness_score_avg
from numbered
group by programme_name, center_id, monthyear
