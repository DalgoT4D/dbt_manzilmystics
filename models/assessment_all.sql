--DBT AUTOMATION has generated this model, please DO NOT EDIT 
--Please make sure you dont change the model name 

{{ config(materialized='table', schema='intermediate') }}
WITH cte1 as (
{{ dbt_utils.union_relations(relations=[source('staging', 'Assessment_2025_26_A56'),source('staging', 'Assessment_2025_26_A12'),source('staging', 'Assessment_2025_26_A34')] , include=['Student_ID','Creativity__1_5_','Class','_airbyte_extracted_at','Pair_ID','Total_Marks','Interaction__1_5_','_airbyte_meta','Year','Involvement__1_5_','_airbyte_raw_id','Emotion__1_5_','Month','Age','Center_ID','Sno_'] , source_column_name=None)}})
-- Final SELECT statement combining the outputs of all CTEs
SELECT *
FROM cte1