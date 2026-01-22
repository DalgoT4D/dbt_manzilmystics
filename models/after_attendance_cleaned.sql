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
CAST("Pre_Assessment" AS character varying) AS "Pre_Assessment",
CAST("Sessions_Absent" AS integer) AS "Sessions_Absent",
CAST("Sessions_Present" AS integer) AS "Sessions_Present",
CAST("_airbyte_raw_id" AS character varying) AS "_airbyte_raw_id",
CAST("_airbyte_extracted_at" AS timestamp with time zone) AS "_airbyte_extracted_at",
CAST("_airbyte_meta" AS jsonb) AS "_airbyte_meta"
FROM {{ref('after_attendance_all')}}
) , cte1 as (
SELECT "Age" as age, 
"Fellow_ID" as fellow_id, "Center_ID" as center_id, 
"Student_ID" as student_id, "Pre_Assessment" as pre_assessment, 
"Sessions_Absent" as sessions_absent,
"Sessions_Present" as sessions_present,
  ("Sessions_Absent" + "Sessions_Present") AS total_sessions,
CASE
  WHEN INITCAP(TRIM("Month")) IN (    'January','February','March','April','May','June','July','August','September','October','November','December'
  )
  THEN TO_DATE(INITCAP(TRIM("Month")) || '-' || "Year", 'Month-YYYY')
  ELSE NULL
END AS "monthyear"  FROM cte2)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1