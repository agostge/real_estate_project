WITH cleaned_data AS (
    {{ generate_listings(19) }}
)

SELECT * FROM cleaned_data
