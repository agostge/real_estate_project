WITH cleaned_data AS (
    {{ generate_listings(6) }}
)

SELECT * FROM cleaned_data
