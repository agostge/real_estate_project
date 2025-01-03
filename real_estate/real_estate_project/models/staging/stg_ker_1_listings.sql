

WITH cleaned_data AS (
    {{ generate_listings(1) }}
)

SELECT * FROM cleaned_data
