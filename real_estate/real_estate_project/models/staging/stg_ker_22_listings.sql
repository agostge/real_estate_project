WITH cleaned_data AS (
    {{ generate_listings(22) }}
)

SELECT * FROM cleaned_data
