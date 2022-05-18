{{ config(materialized='view') }}

with icustay as (
    select subject_id,
    hadm_id,
    icustay_id,
    gender,
    dod,
    cast(admittime as TIMESTAMP) as admittime,
    cast(dischtime as TIMESTAMP) as dischtime,
    los_hospital,
    admission_age,
    ethnicity,
    ethnicity_grouped,
    hospital_expire_flag, 	
    hospstay_seq,
    first_hosp_stay,
    cast(intime as TIMESTAMP) as intime,
    cast(outtime as TIMESTAMP) as outtime,
    los_icu,
    icustay_seq,
    first_icu_stay 
    from mimiciii_derived.icustay_detail
)

select glc.*,
    icu.gender,icu.dod,icu.admittime,icu.dischtime,icu.los_hospital,icu.admission_age,
    icu.ethnicity,icu.ethnicity_grouped,icu.hospital_expire_flag,icu.hospstay_seq,
    icu.first_hosp_stay,icu.intime,icu.outtime,icu.los_icu,icu.icustay_seq,icu.first_icu_stay 
from 
{{ ref('stg_glucose_insulin_concat') }} glc
left join 
icustay icu
on glc.SUBJECT_ID=icu.SUBJECT_ID AND
    glc.HADM_ID=icu.HADM_ID and 
    glc.GLCTIMER > icu.intime and 
    glc.GLCTIMER < icu.outtime and
    glc.STARTTIME < icu.intime and 
    glc.ENDTIME < icu.outtime