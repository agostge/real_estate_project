WITH cleaned_data AS (
    {{ generate_listings(11) }}
)

SELECT * FROM cleaned_data
