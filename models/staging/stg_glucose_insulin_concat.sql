{{ config(materialized='view') }}

with glucose as (
    select SUBJECT_ID,
    HADM_ID,
    stay_id,
        charttime as GLCTIMER,
        glucose as glc,
        glc_source,
            CAST(NULL as TIMESTAMP) as STARTTIME,
            CAST(NULL as TIMESTAMP) as ENDTIME,
            NULL as INPUT,
            NULL as INPUT_HRS,
            CAST(NULL as STRING) as INSULINTYPE,
            CAST(NULL as STRING) as EVENT,
            NULL as INFXSTOP
    from {{ ref('stg_glucose_clean') }}
),
insulin as (
        select SUBJECT_ID,
            HADM_ID,
            stay_id,
            CAST(NULL as TIMESTAMP) as GLCTIMER,
            NULL as GLC,
            CAST(NULL as STRING) as glc_source,
            STARTTIME,
            ENDTIME,
            AMOUNT as INPUT,
            RATE as INPUT_HRS,
            InsulinType as InsulinType,
            InsulinAdmin as EVENT,
            INFXSTOP 
    from {{ ref('stg_insulin_clean') }}
)

select SUBJECT_ID,HADM_ID,stay_id,GLCTIMER,GLC,glc_source,STARTTIME,ENDTIME,INPUT,INPUT_HRS,InsulinType,EVENT,INFXSTOP
from glucose
union all
select SUBJECT_ID,HADM_ID,stay_id,GLCTIMER,GLC,glc_source,STARTTIME,ENDTIME,INPUT,INPUT_HRS,InsulinType,EVENT,INFXSTOP
from insulin
