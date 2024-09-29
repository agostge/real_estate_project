WITH cleaned_data AS (
    {{ generate_listings(12) }}
)

SELECT * FROM cleaned_data
