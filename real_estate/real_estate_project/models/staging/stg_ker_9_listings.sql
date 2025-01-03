WITH cleaned_data AS (
    {{ generate_listings(9) }}
)

SELECT * FROM cleaned_data
