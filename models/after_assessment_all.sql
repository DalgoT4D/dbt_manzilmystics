--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
{{ dbt_utils.union_relations(relations=[source('staging', 'Assessment_2025_26_12'),source('staging', 'Assessment_2025_26_7'),source('staging', 'Assessment_2025_26_8'),source('staging', 'Assessment_2025_26_9'),source('staging', 'Assessment_2025_26_10'),source('staging', 'Assessment_2025_26_11')] , include=['Student_ID','Fellow_ID','Student_s_Attendance__1_5_','_airbyte_extracted_at','_airbyte_meta','Hold_on_Laya__1_5_','Year','Engagement_Level__Volunteers_in_events__responding_on_WhatsApp_','_airbyte_raw_id','Readiness_to_Graduate___Music_Skill__Vocal__Instrument__1_10','Month','Age','Effort__Homework__quickly_learns__memorize_lessons__1_10','Center_ID','Sno_','Performance_Readiness__Confidence__clarity_in_explaining__prese','Total_Score_out_of_50'] , source_column_name=None)}})
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1