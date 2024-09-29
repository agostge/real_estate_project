WITH cleaned_data AS (
    {{ generate_listings(7) }}
)

SELECT * FROM cleaned_data
