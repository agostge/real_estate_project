WITH cleaned_data AS (
    {{ generate_listings(16) }}
)

SELECT * FROM cleaned_data
