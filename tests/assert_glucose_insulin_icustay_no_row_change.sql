
with tmp as (
    select hadm_id,GLCTIMER,count(*) as n
    from {{ ref('stg_glucose_insulin_icustay') }}
    group by hadm_id,GLCTIMER
    having n > 1
)
select *
from {{ ref('stg_glucose_insulin_icustay') }}
where hadm_id in (
    select hadm_id from tmp where n > 1
)
