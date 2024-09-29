WITH cleaned_data AS (
    {{ generate_listings(17) }}
)

SELECT * FROM cleaned_data
