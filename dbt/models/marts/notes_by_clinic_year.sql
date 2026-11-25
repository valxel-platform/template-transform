-- Notes per clinic and year. Counts over patients are only shown for groups of at least 11 notes.
select
    clinic_id,
    {{ valxel_generalize_year('signed_at') }} as year,
    count(*) as notes,
    count(distinct patient_id) as patients
from {{ ref('stg_notes') }}
where signed_at is not null
group by 1, 2
{{ valxel_min_group(11) }}
