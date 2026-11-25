-- One staging model per raw table: rename, cast, deduplicate. Nothing else.
select
    note_id,
    kind,
    clinic_id,
    patient_id,
    signed_at,
    _updated_at
from {{ source('reporting', 'notes') }}
where note_id is not null
