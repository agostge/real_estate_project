
import pandas as pd

from utils.constants import get_postgres_connection






  

def extract_data_from_postgres(district):
    conn = get_postgres_connection()
    
    cur = conn.cursor()

    query = f'SELECT * FROM bronze_listings.ker_{district}_listings'
    cur.execute(query)
    # Convert data into a pandas DataFrame
    df = pd.DataFrame(cur.fetchall(), columns=['identifier','room','area','price','district','date_updated','is_active'])
    print(f"Data from postgres loaded for {district}")
    
    return df


def load_data_to_postgres(data, district):
    conn = get_postgres_connection()
    
    cur = conn.cursor()
    
    # Create schema if it doesn't exist
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS bronze_listings")
    
    # Create table query with schema and dynamic district name
    create_table_query = f'''
    CREATE TABLE IF NOT EXISTS bronze_listings.Ker_{district}_listings (
        identifier VARCHAR(255) PRIMARY KEY,
        room VARCHAR(255),
        area VARCHAR(255),
        price VARCHAR(255),
        district INTEGER,
        date_updated VARCHAR(255),
        is_active VARCHAR(255)
    )
    '''
    
    cur.execute(create_table_query)
    
    # Insert the transformed data into the dynamically created table
    insert_query = f'''
    INSERT INTO bronze_listings.Ker_{district}_listings (identifier, room, area, price, district,date_updated, is_active)
    VALUES (%s, %s, %s, %s, %s,%s, %s)
    ON CONFLICT (identifier)
    DO UPDATE SET 
        price = Excluded.price,
        date_updated = Excluded.date_updated,
        is_active = Excluded.is_active


    '''
    
    for _, row in data.iterrows():
        cur.execute(insert_query, (row['identifier'], row['room'], row['area'], row['price'], district, row['date_updated'], row['is_active']))
    
    conn.commit()
    
    cur.close()
    conn.close()

    print(f"Data loaded to PostgreSQL successfully for district {district}.")




def detect_changes(new_data, existing_data):
    # Find identifiers in existing data that are not present in the new data (removed)
    removed_identifiers = existing_data[
    (~existing_data['identifier'].isin(new_data['identifier'])) & (existing_data['is_active'] != 'inactive')]
    if not removed_identifiers.empty:
        # Update the 'date_removed' column for the removed identifiers
        removed_identifiers.loc[existing_data['identifier'].isin(removed_identifiers['identifier']), 'date_updated'] = pd.Timestamp.today().strftime('%Y-%m-%d')
        removed_identifiers.loc[existing_data['identifier'].isin(removed_identifiers['identifier']), 'is_active'] = 'Inactive'
    
    # Find rows where the identifier is present in both datasets, but the price is different
    merged_data = pd.merge(new_data, existing_data, on='identifier', how='inner', suffixes=('_new', '_existing'))
    price_diff = merged_data[merged_data['price_new'] != merged_data['price_existing']].copy()
    if not price_diff.empty:
        # For rows with price differences, append the new row with the updated price and date
        price_diff['date_updated'] = pd.Timestamp.today().strftime('%Y-%m-%d')
        price_diff.drop(columns=['room_existing','area_existing','price_existing'], inplace=True)
        price_diff.rename(columns= {'room_new':'room','area_new':'area','price_new':'price','is_active_new':'is_active'}, inplace=True)
    else:
        price_diff.drop(columns=['room_existing','area_existing','price_existing'], inplace=True)
        price_diff.rename(columns= {'room_new':'room','area_new':'area','price_new':'price','is_active_new':'is_active','district_new':'district'}, inplace=True)
   

    
    # Print the results (for debugging purposes)
    if not removed_identifiers.empty:
        print(f"Removed Identifiers:\n{removed_identifiers}")
    
    if not price_diff.empty:
        print(f"Updated Data with Price Changes:\n{price_diff}")

    # Combine the changes into a dataframe 
    updated_existing_data = pd.concat([removed_identifiers, price_diff]).drop_duplicates(subset='identifier', keep='last')

    return         updated_existing_data


