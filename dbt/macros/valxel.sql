{# The template's macros. transform-check knows each one: what it compiles to is how the data-class
   check sees a column de-identified. Don't redefine them in models. #}

{# A keyed token of a value (HMAC-SHA256 with the session's key): the same input gives the same token,
   so marts still join on it, but the value can't be read back without the key. De-identifies. #}
{% macro valxel_tokenize(column) -%}
  valxel.tokenize(({{ column }})::text)
{%- endmacro %}

{# A date or time cut to its year (HIPAA safe harbor keeps the year): de-identifies. Any finer cut
   keeps the column's class; use date_trunc for that. #}
{% macro valxel_generalize_year(column) -%}
  valxel.generalize_year(({{ column }})::timestamptz)
{%- endmacro %}

{# The group-size floor for an aggregate mart: groups smaller than this are left out, so a count or
   sum can't single out a person. transform-check reads it; the floor for PHI is 11. #}
{% macro valxel_min_group(n=11) -%}
  having count(*) >= {{ n }}
{%- endmacro %}

{# Incremental models: after each run, remove what's gone at the source (reporting deletes a raw row
   on a .deleted event). Quote it, so it renders when the model runs, not when the project is parsed:
     post_hook=["{{ valxel_deletions(source('reporting', 'notes'), 'note_id') }}"]
   Unquoted, source() and this resolve at parse time and the hook removes nothing. #}
{% macro valxel_deletions(source_relation, source_key, key=none) -%}
  delete from {{ this }} as t
  where not exists (select 1 from {{ source_relation }} as s where s.{{ source_key }} = t.{{ key or source_key }})
{%- endmacro %}

{# Control total: a measure on this model against the same measure on what it's built from, within a
   tolerance. compare: equal (the default), or at_most when the model leaves rows out on purpose
   (an aggregate's small groups). Blocking, like every test here. #}
{% test valxel_control_total(model, measure, against, against_measure=none, where=none, against_where=none,
                             tolerance=0, compare='equal') %}
  with mine as (
    select ({{ measure }})::numeric as v from {{ model }} {% if where %} where {{ where }} {% endif %}
  ), theirs as (
    select ({{ against_measure or measure }})::numeric as v from {{ against }}
    {% if against_where %} where {{ against_where }} {% endif %}
  )
  select mine.v as model_value, theirs.v as source_value
  from mine, theirs
  where
  {% if compare == 'at_most' %}
    coalesce(mine.v, 0) > coalesce(theirs.v, 0) + {{ tolerance }}
  {% else %}
    abs(coalesce(mine.v, 0) - coalesce(theirs.v, 0)) > {{ tolerance }}
  {% endif %}
{% endtest %}
