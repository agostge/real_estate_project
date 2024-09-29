{% snapshot scd_raw_listings %}
{{
config(
target_schema='listings_snapshot',
unique_key='identifier',
strategy='timestamp',
updated_at='date_updated',
invalidate_hard_deletes=True,
materialized='snapshot' 
)
}}
select * FROM {{ ref('final_listings') }}
{% endsnapshot %}