{% macro generate_listings(district) %}
    SELECT 
        identifier,
        -- clean szoba
        CASE 
            WHEN room LIKE '%-%' THEN 
                CAST(rtrim(split_part(room, ' - ', 2), ' szoba') AS integer)
            ELSE
                CAST(rtrim(room, ' szoba') AS integer)
        END AS room,
        -- clean terulet
        CASE 
            WHEN area LIKE '%-%' THEN 
                CAST(rtrim(split_part(area, ' - ', 2), ' m²') AS integer)
            ELSE
                CAST(rtrim(area, ' m²') AS integer)
        END AS "area(m²)",
        -- clean ar
        CASE 
            WHEN price LIKE '%-%' THEN 
                CAST(trim( ' M Ft' from split_part(price, ' - ', 2))  AS integer) * 1000000
            ELSE
                CAST(replace(rtrim(price, 'Ft'),' ','') AS integer)
        END AS "price(Huf)",
        district,
        to_date(date_updated,'YYYY-MM-DD') as date_updated,
        is_active

    FROM {{ source('raw_layer', 'ker_' ~ district ~ '_listings') }}
{% endmacro %}
