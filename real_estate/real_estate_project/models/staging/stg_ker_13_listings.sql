WITH cleaned_data AS (
    {{ generate_listings(13) }}
)

SELECT * FROM cleaned_data
