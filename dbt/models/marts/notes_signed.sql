-- Signed notes for analysis outside the care team: the patient as a token, the date as a year.
-- Incremental: each run adds what changed since the last, and the deletions hook removes notes
-- that are gone at the source.
{{ config(
    materialized='incremental',
    unique_key='note_id',
    post_hook=["{{ valxel_deletions(source('reporting', 'notes'), 'note_id') }}"],
) }}
select
    note_id,
    kind,
    clinic_id,
    {{ valxel_tokenize('patient_id') }} as patient_token,
    {{ valxel_generalize_year('signed_at') }} as signed_year,
    _updated_at
from {{ ref('stg_notes') }}
where signed_at is not null
{% if is_incremental() %}
  and _updated_at > (select coalesce(max(_updated_at), '-infinity'::timestamptz) from {{ this }})
{% endif %}
