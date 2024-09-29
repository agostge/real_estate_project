WITH cleaned_data AS (
    {{ generate_listings(15) }}
)

SELECT * FROM cleaned_data
