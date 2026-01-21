--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
{{ dbt_utils.union_relations(relations=[source('staging', 'Attendance_2025_26_A12'),source('staging', 'Attendance_2025_26_A34'),source('staging', 'Attendance_2025_26_A56')] , include=['Month','Sessions_Absent','Pair_ID','Student_ID','_airbyte_raw_id','_airbyte_extracted_at','Pre_Assessment','Class','Year','Age','_airbyte_meta','Sno_','Sessions_Present','Center_ID'] , source_column_name=None)}})
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1