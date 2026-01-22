{{ config(materialized='table', schema='production') }}

with
inschool as (
    select
        programme_name,
        student_id,
        center_id,
        month_year as monthyear,
        age,
        class,
        pair_id,
        cast(null as text) as fellow_id,
        "Pre_Assessment" as pre_assessment,
        sessions_absent,
        sessions_present,
        total_sessions,
        total_marks,
        emotionscore,
        creativityscore,
        interactionscore,
        involvementscore,
        cast(null as integer) as total_score_50,
        cast(null as integer) as holdonlaya_score,
        cast(null as integer) as attendance_score,
        cast(null as integer) as effort_homework_memorize_score,
        cast(null as integer) as readinesstograd_score,
        cast(null as integer) as engagementwhatsapp_score,
        cast(null as integer) as performance_readiness_score
    from {{ ref('inschool_all') }}
),
afterschool as (
    select
        programme_name,
        student_id,
        center_id,
        monthyear,
        age,
        cast(null as integer) as class,
        cast(null as text) as pair_id,
        fellow_id,
        pre_assessment,
        sessions_absent,
        sessions_present,
        total_sessions,
        cast(null as integer) as total_marks,
        cast(null as integer) as emotionscore,
        cast(null as integer) as creativityscore,
        cast(null as integer) as interactionscore,
        cast(null as integer) as involvementscore,
        total_score_50,
        holdonlaya_score,
        attendance_score,
        effort_homework_memorize_score,
        readinesstograd_score,
        engagementwhatsapp_score,
        performance_readiness_score
    from {{ ref('afterschool_all') }}
)

select *
from inschool
union all
select *
from afterschool
