{{ config(materialized='view') }}

with fingerstick as (
    select DISTINCT *
    from {{ ref('stg_glucose_raw') }}
    where ITEM_GLC in (807,811,1529,225664,226537) AND
        	glucose < 500
),
lab as (
    select DISTINCT *
    from {{ ref('stg_glucose_raw') }}
    where ITEM_GLC in (3745,220621,50931,50809) AND
        	glucose < 1000
)

select SUBJECT_ID,HADM_ID,stay_id,
        CASE WHEN charttime < storetime THEN charttime ELSE storetime END as charttime,
        STORETIME,glucose,ITEM_GLC,glc_source
from (
    select SUBJECT_ID,HADM_ID,stay_id,CHARTTIME,STORETIME,glucose,ITEM_GLC,'fingerstick' as glc_source
    from fingerstick
    union all
    select SUBJECT_ID,HADM_ID,stay_id,CHARTTIME,STORETIME,glucose,ITEM_GLC,'lab' as glc_source
    from lab
) tab