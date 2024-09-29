WITH cleaned_data AS (
    {{ generate_listings(21) }}
)

SELECT * FROM cleaned_data
