--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte2 as (
SELECT
CAST("Age" AS integer) AS "Age",
CAST("Sno_" AS character varying) AS "Sno_",
CAST("Year" AS character varying) AS "Year",
CAST("Month" AS character varying) AS "Month",
CAST("Center_ID" AS character varying) AS "Center_ID",
CAST("Fellow_ID" AS character varying) AS "Fellow_ID",
CAST("Student_ID" AS character varying) AS "Student_ID",
CAST("Hold_on_Laya__1_5_" AS integer) AS "Hold_on_Laya__1_5_",
CAST("Total_Score_out_of_50" AS integer) AS "Total_Score_out_of_50",
CAST("Student_s_Attendance__1_5_" AS integer) AS "Student_s_Attendance__1_5_",
CAST("Effort__Homework__quickly_learns__memorize_lessons__1_10" AS integer) AS "Effort__Homework__quickly_learns__memorize_lessons__1_10",
CAST("Readiness_to_Graduate___Music_Skill__Vocal__Instrument__1_10" AS integer) AS "Readiness_to_Graduate___Music_Skill__Vocal__Instrument__1_10",
CAST("Engagement_Level__Volunteers_in_events__responding_on_WhatsApp_" AS integer) AS "Engagement_Level__Volunteers_in_events__responding_on_WhatsApp_",
CAST("Performance_Readiness__Confidence__clarity_in_explaining__prese" AS integer) AS "Performance_Readiness__Confidence__clarity_in_explaining__prese",
CAST("_airbyte_raw_id" AS character varying) AS "_airbyte_raw_id",
CAST("_airbyte_extracted_at" AS timestamp with time zone) AS "_airbyte_extracted_at",
CAST("_airbyte_meta" AS jsonb) AS "_airbyte_meta"
FROM {{ref('after_assessment_all')}}
) , cte1 as (
SELECT "Age" as age, 
"Fellow_ID" as fellow_id, "Center_ID" as center_id, 
"Student_ID" as student_id, 
"Total_Score_out_of_50" as total_score_50,
"Hold_on_Laya__1_5_" as holdonlaya_score,
"Student_s_Attendance__1_5_" as attendance_score,
"Effort__Homework__quickly_learns__memorize_lessons__1_10" as effort_homework_memorize_score,
"Readiness_to_Graduate___Music_Skill__Vocal__Instrument__1_10" as "readinesstograd_score",
"Engagement_Level__Volunteers_in_events__responding_on_WhatsApp_" as engagementwhatsapp_score,
"Performance_Readiness__Confidence__clarity_in_explaining__prese" as performance_readiness_score,
CASE
  WHEN INITCAP(TRIM("Month")) IN (    'January','February','March','April','May','June','July','August','September','October','November','December'
  )
  THEN TO_DATE(INITCAP(TRIM("Month")) || '-' || "Year", 'Month-YYYY')
  ELSE NULL
END AS "monthyear"  FROM cte2)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1