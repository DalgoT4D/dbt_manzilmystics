--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
{{ dbt_utils.union_relations(relations=[source('staging', 'Attendance_2025_26_7'),source('staging', 'Attendance_2025_26_8'),source('staging', 'Attendance_2025_26_9'),source('staging', 'Attendance_2025_26_10'),source('staging', 'Attendance_2025_26_11'),source('staging', 'Attendance_2025_26_12')] , include=['Student_ID','Fellow_ID','Sessions_Present','_airbyte_extracted_at','_airbyte_meta','Year','_airbyte_raw_id','Pre_Assessment','Sessions_Absent','Month','Age','Center_ID','Sno_'] , source_column_name=None)}})
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1