WITH cleaned_data AS (
    {{ generate_listings(2) }}
)

SELECT * FROM cleaned_data
