WITH cleaned_data AS (
    {{ generate_listings(5) }}
)

SELECT * FROM cleaned_data
