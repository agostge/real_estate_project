{{
    config(
        materialized='incremental',
        unique_key='identifier')
}}

with final as (
    select * from {{ ref('stg_ker_1_listings') }}
UNION ALL
select * from {{ ref('stg_ker_2_listings') }}
UNION ALL
select * from {{ ref('stg_ker_3_listings') }}
UNION ALL
select * from {{ ref('stg_ker_4_listings') }}
UNION ALL
select * from {{ ref('stg_ker_5_listings') }}
UNION ALL
select * from {{ ref('stg_ker_6_listings') }}
UNION ALL
select * from {{ ref('stg_ker_7_listings') }}
UNION ALL
select * from {{ ref('stg_ker_8_listings') }}
UNION ALL
select * from {{ ref('stg_ker_9_listings') }}
UNION ALL
select * from {{ ref('stg_ker_10_listings') }}
UNION ALL
select * from {{ ref('stg_ker_11_listings') }}
UNION ALL
select * from {{ ref('stg_ker_12_listings') }}
UNION ALL
select * from {{ ref('stg_ker_13_listings') }}
UNION ALL
select * from {{ ref('stg_ker_14_listings') }}
UNION ALL
select * from {{ ref('stg_ker_15_listings') }}
UNION ALL
select * from {{ ref('stg_ker_16_listings') }}
UNION ALL
select * from {{ ref('stg_ker_17_listings') }}
UNION ALL
select * from {{ ref('stg_ker_18_listings') }}
UNION ALL
select * from {{ ref('stg_ker_19_listings') }}
UNION ALL
select * from {{ ref('stg_ker_20_listings') }}
UNION ALL
select * from {{ ref('stg_ker_21_listings') }}
UNION ALL
select * from {{ ref('stg_ker_22_listings') }}
UNION ALL
select * from {{ ref('stg_ker_23_listings') }}

)

select * from final

{% if is_incremental()%}

    WHERE date_updated > (SELECT MAX(date_updated) FROM {{ this }})
{% endif%}    










