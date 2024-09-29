WITH cleaned_data AS (
    {{ generate_listings(8) }}
)

SELECT * FROM cleaned_data
