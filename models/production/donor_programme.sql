{{ config(materialized='table', schema='production') }}

with
donor_programme_list as (
    select distinct
        "Donor"::text as donor,
        upper(trim("Programme"::text)) as programme
    from {{ source('staging', 'Donor_List') }}
    where "Donor" is not null
      and "Programme" is not null
      and upper(trim("Programme"::text)) in ('IS', 'AS', 'LTM')
),
studentlevel as (
    select *
    from {{ ref('ltm_studentlevel') }}
),
expanded as (
    select
        dpl.donor,
        dpl.programme,
        sl.*
    from donor_programme_list dpl
    join studentlevel sl
        on (
            (dpl.programme = 'IS' and sl.programme_name = 'in school')
            or (dpl.programme = 'AS' and sl.programme_name = 'after school')
            or (dpl.programme = 'LTM')
        )
),
typed as (
    select
        *,
        case
            when pre_assessment is null then null
            when lower(trim(pre_assessment::text)) in ('true', 't', 'yes', 'y', '1') then true
            when lower(trim(pre_assessment::text)) in ('false', 'f', 'no', 'n', '0') then false
            else null
        end as pre_assessment_bool
    from expanded
)

select
    donor,
    programme,
    case
        when programme = 'IS' then 'in school'
        when programme = 'AS' then 'after school'
        when programme = 'LTM' then 'ltm (in + after)'
        else programme
    end as programme_label,

    max(monthyear) as latest_monthyear,

    count(distinct student_id) as total_students,
    count(distinct case when programme_name = 'in school' then student_id end) as total_students_in_school,
    count(distinct case when programme_name = 'after school' then student_id end) as total_students_after_school,
    count(distinct center_id) as unique_centres,
    count(distinct pair_id) as unique_pair_count,
    count(distinct fellow_id) as unique_fellow_count,

    round(
        sum(coalesce(sessions_present, 0))::numeric
        / nullif(sum(coalesce(sessions_present, 0) + coalesce(sessions_absent, 0)), 0),
        4
    ) * 100 as attendance_pct_avg,

    round(
        sum(case when pre_assessment_bool is true then 1 else 0 end)::numeric
        / nullif(sum(case when pre_assessment_bool is not null then 1 else 0 end), 0),
        4
    ) * 100 as pre_assessment_complete_pct,

    round(avg(total_marks), 2) as total_marks_avg,
    round(avg(emotionscore), 2) as emotionscore_avg,
    round(avg(creativityscore), 2) as creativityscore_avg,
    round(avg(interactionscore), 2) as interactionscore_avg,
    round(avg(involvementscore), 2) as involvementscore_avg,

    round(avg(total_score_50), 2) as total_score_50_avg,
    round(avg(holdonlaya_score), 2) as holdonlaya_score_avg,
    round(avg(attendance_score), 2) as attendance_score_avg,
    round(avg(effort_homework_memorize_score), 2) as effort_homework_memorize_score_avg,
    round(avg(readinesstograd_score), 2) as readinesstograd_score_avg,
    round(avg(engagementwhatsapp_score), 2) as engagementwhatsapp_score_avg,
    round(avg(performance_readiness_score), 2) as performance_readiness_score_avg,

    count(distinct case when total_marks > 10 then student_id end) as students_total_marks_over_10,
    count(distinct case when total_score_50 > 10 then student_id end) as students_total_score_50_over_10
from typed
group by donor, programme
