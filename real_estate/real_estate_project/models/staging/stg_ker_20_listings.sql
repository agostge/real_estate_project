WITH cleaned_data AS (
    {{ generate_listings(20) }}
)

SELECT * FROM cleaned_data
