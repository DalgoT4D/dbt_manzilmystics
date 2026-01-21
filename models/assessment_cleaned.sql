--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='production') }}
WITH cte2 as (
SELECT
CAST("Age" AS integer) AS "Age",
CAST("Sno_" AS character varying) AS "Sno_",
CAST("Year" AS character varying) AS "Year",
CAST("Class" AS integer) AS "Class",
CAST("Month" AS character varying) AS "Month",
CAST("Pair_ID" AS character varying) AS "Pair_ID",
CAST("Center_ID" AS character varying) AS "Center_ID",
CAST("Student_ID" AS character varying) AS "Student_ID",
CAST("Total_Marks" AS integer) AS "Total_Marks",
CAST("Emotion__1_5_" AS integer) AS "Emotion__1_5_",
CAST("Creativity__1_5_" AS integer) AS "Creativity__1_5_",
CAST("Interaction__1_5_" AS integer) AS "Interaction__1_5_",
CAST("Involvement__1_5_" AS integer) AS "Involvement__1_5_",
CAST("_airbyte_raw_id" AS character varying) AS "_airbyte_raw_id",
CAST("_airbyte_extracted_at" AS timestamp with time zone) AS "_airbyte_extracted_at",
CAST("_airbyte_meta" AS jsonb) AS "_airbyte_meta"
FROM {{ref('assessment_all')}}
) , cte1 as (
SELECT "Age", "Class", "Pair_ID", "Center_ID", 
"Student_ID", 
"Total_Marks",
"Emotion__1_5_",
"Creativity__1_5_",
"Interaction__1_5_",
"Involvement__1_5_",

CASE
  WHEN INITCAP(TRIM("Month")) IN (    'January','February','March','April','May','June','July','August','September','October','November','December'
  )
  THEN TO_DATE(INITCAP(TRIM("Month")) || '-' || "Year", 'Month-YYYY')
  ELSE NULL
END AS "MonthYear"  FROM cte2)
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1