WITH cleaned_data AS (
    {{ generate_listings(18) }}
)

SELECT * FROM cleaned_data
