WITH cleaned_data AS (
    {{ generate_listings(10) }}
)

SELECT * FROM cleaned_data
