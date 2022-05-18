{{ config(materialized='view') }}

with icustay as (
    select subject_id,
    hadm_id,
    stay_id,
    gender,
    dod,
    cast(admittime as TIMESTAMP) as admittime,
    cast(dischtime as TIMESTAMP) as dischtime,
    los_hospital,
    admission_age,
    ethnicity,
    hospital_expire_flag, 	
    hospstay_seq,
    first_hosp_stay,
    cast(icu_intime as TIMESTAMP) as icu_intime,
    cast(icu_outtime as TIMESTAMP) as icu_outtime,
    los_icu,
    icustay_seq,
    first_icu_stay 
    from mimic_derived.icustay_detail
),
glucose_icustay as (
    select glc.*,
        icu1.gender,icu1.dod,icu1.admittime,icu1.dischtime,icu1.los_hospital,icu1.admission_age,
        icu1.ethnicity,icu1.hospital_expire_flag,icu1.hospstay_seq,
        icu1.first_hosp_stay,icu1.icu_intime,icu1.icu_outtime,icu1.los_icu,icu1.icustay_seq,icu1.first_icu_stay 
    from 
    (select *
    from {{ ref('stg_glucose_insulin_concat') }}
    where GLCTIMER is not null) as glc
    join 
    icustay icu1
    on glc.SUBJECT_ID=icu1.SUBJECT_ID AND
        glc.HADM_ID=icu1.HADM_ID and 
        glc.GLCTIMER > icu1.icu_intime and 
        glc.GLCTIMER < icu1.icu_outtime
),
insulin_icustay as (
        select ins.*,
        icu2.gender,icu2.dod,icu2.admittime,icu2.dischtime,icu2.los_hospital,icu2.admission_age,
        icu2.ethnicity,icu2.hospital_expire_flag,icu2.hospstay_seq,
        icu2.first_hosp_stay,icu2.icu_intime,icu2.icu_outtime,icu2.los_icu,icu2.icustay_seq,icu2.first_icu_stay 
    from 
    (select *
    from {{ ref('stg_glucose_insulin_concat') }}
    where STARTTIME is not null) as ins
    join 
    icustay icu2
    on ins.SUBJECT_ID=icu2.SUBJECT_ID AND
        ins.HADM_ID=icu2.HADM_ID and 
        ins.STARTTIME > icu2.icu_intime and 
        ins.STARTTIME < icu2.icu_outtime
)

select * from (
    select *
    from glucose_icustay
    union distinct
    select *
    from insulin_icustay
)

