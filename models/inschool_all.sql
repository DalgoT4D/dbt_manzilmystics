{{ config(materialized='table', schema='intermediate') }}



with
assessment as (
    select
        "Student_ID",
        "Pair_ID",
        "Center_ID",
        "MonthYear",
        "Age",
        "Class",
        "Total_Marks",
        "Emotion__1_5_",
        "Creativity__1_5_",
        "Interaction__1_5_",
        "Involvement__1_5_"
    from {{ ref('assessment_cleaned') }}
),
attendance as (
    select
        "Student_ID",
        "Pair_ID",
        "Center_ID",
        "MonthYear",
        "Age",
        "Class",
        "Pre_Assessment",
        "Sessions_Absent",
        "Sessions_Present",
        total_sessions
    from {{ ref('attendance_cleaned') }}
)

select
    a."Student_ID" as student_id,
    a."Pair_ID" as pair_id,
    a."Center_ID" as center_id,
    a."MonthYear" as month_year,
    a."Age" as age,
    a."Class" as class,
    att."Pre_Assessment",
    att."Sessions_Absent" as sessions_present,
    att."Sessions_Present" as sessions_absent,
    att.total_sessions,
    a."Total_Marks" as total_marks,
    a."Emotion__1_5_" as emotionscore,
    a."Creativity__1_5_" as creativityscore,
    a."Interaction__1_5_" as interactionscore,
    a."Involvement__1_5_" as involvementscore,
    'in school' as programme_name
from assessment a
left join attendance att
    on a."Student_ID" = att."Student_ID"
    and a."Pair_ID" = att."Pair_ID"
    and a."Center_ID" = att."Center_ID"
    and a."MonthYear" = att."MonthYear"
    and a."Age" = att."Age"
    and a."Class" = att."Class"