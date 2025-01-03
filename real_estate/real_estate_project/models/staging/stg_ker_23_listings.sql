WITH cleaned_data AS (
    {{ generate_listings(23) }}
)

SELECT * FROM cleaned_data
