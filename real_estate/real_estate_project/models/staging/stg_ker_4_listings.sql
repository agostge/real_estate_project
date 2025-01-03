WITH cleaned_data AS (
    {{ generate_listings(4) }}
)

SELECT * FROM cleaned_data
