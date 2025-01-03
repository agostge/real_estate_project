WITH cleaned_data AS (
    {{ generate_listings(3) }}
)

SELECT * FROM cleaned_data
