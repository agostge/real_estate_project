WITH cleaned_data AS (
    {{ generate_listings(14) }}
)

SELECT * FROM cleaned_data
