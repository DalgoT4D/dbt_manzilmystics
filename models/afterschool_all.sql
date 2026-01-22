--DBT AUTOMATION has generated this model, please DO NOT EDIT
--Please make sure you dont change the model name

{{ config(materialized='table', schema='intermediate') }}

with
after_assessment as (
    select
        student_id,
        fellow_id,
        center_id,
        monthyear,
        age,
        total_score_50,
        holdonlaya_score,
        attendance_score,
        effort_homework_memorize_score,
        "readinesstograd_score" as readinesstograd_score,
        engagementwhatsapp_score,
        performance_readiness_score
    from {{ ref('after_assessment_cleaned') }}
),
after_attendance as (
    select
        student_id,
        age,
        fellow_id,
        center_id,
        monthyear,
        pre_assessment,
        sessions_absent,
        sessions_present,
        total_sessions
    from {{ ref('after_attendance_cleaned') }}
)

select
    a.student_id,
    a.fellow_id,
    a.center_id,
    a.monthyear,
    a.age,
    att.pre_assessment,
    att.sessions_absent,
    att.sessions_present,
    att.total_sessions,
    a.total_score_50,
    a.holdonlaya_score,
    a.attendance_score,
    a.effort_homework_memorize_score,
    a.readinesstograd_score,
    a.engagementwhatsapp_score,
    a.performance_readiness_score,
    'after school' as programme_name
from after_assessment a
inner join after_attendance att
    on a.student_id = att.student_id
    and a.fellow_id = att.fellow_id
    and a.center_id = att.center_id
    and a.monthyear = att.monthyear
    and a.age = att.age
