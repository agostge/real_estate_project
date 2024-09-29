import requests
import re

from bs4 import BeautifulSoup
from airflow import DAG
from datetime import datetime, timedelta

import pandas as pd

from utils.helper_functions import  extract_data_from_postgres, load_data_to_postgres
from utils.helper_functions import  detect_changes

from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup



default_args = {
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 24, 18),  # Update the start date
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # Retry after 5 minutes in case of failure
}

def fetch_page_content( url: str) -> str:
    """Fetches the HTML content of the given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        print(f"Fetched page content for {url}")
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None

def get_last_page_number( page_content: str) -> int:
    """Extracts the last page number from the HTML content."""
    soup = BeautifulSoup(page_content, "html.parser")
    last_page_link = soup.find(
        'a', 
        class_='bg-white rounded-[32px] border-solid border-text-stroke border flex items-center justify-center shrink-0 w-8 h-8 cursor-pointer'
    )
    if last_page_link:
        href_value = last_page_link['href']
        match = re.search(r'page=(\d+)', href_value)
        if match:
            return int(match.group(1))
    return 1  # Default to 1 if no page number is found

def scrape_values(base_url: str, last_page_number: int) -> pd.DataFrame:
    """Scrapes all property identifiers from the paginated search results."""
    all_identifiers = []
    all_pricetags = []
    
    for page in range(1, last_page_number + 1):
        page_url = f"{base_url}?page={page}"
        page_content = fetch_page_content(page_url)
        if page_content:
            soup = BeautifulSoup(page_content, "html.parser")
            listings = soup.find_all(
                'a', 
                class_="rounded-[20px] bg-white flex flex-col justify-start self-stretch shrink-0 w-full shadow-thin hover:shadow-thin-hover transition overflow-hidden"
            )
            
            for listing in listings:
                identifier = listing.find('div', class_='text-sm font-light min-h-[40px] flex items-center -my-2')
                pricetag = listing.find('div', class_='text-primary-magenta text-2xl font-bold')

                if identifier:
                    identifier_data = [i.strip() for i in identifier.text.split('|')]
                    #if len(identifier_data) == 3 and len(identifier_data[0]) < 10:   Ensure correct data format
                    all_identifiers.append(identifier_data)
                if pricetag:
                    all_pricetags.append(pricetag.text.strip())
        
    identifiers_df = pd.DataFrame(all_identifiers, columns=['identifier', 'room', 'area'])
    pricetags_df = pd.DataFrame(all_pricetags, columns=['price'])

    final_df = pd.concat([identifiers_df,pricetags_df],axis=1)

    final_df.drop(final_df[final_df['identifier'].str.len() > 10].index, inplace=True)
    return final_df

def upload_to_postgres( new_data: pd.DataFrame, district: int):
    """Uploads DataFrame to Postgres after comparing it with the existing data (if exists)."""
    try:
        existing_data = extract_data_from_postgres(district)
    except Exception as e:
        print(f"No existing data found for district {district}. Skipping comparison.")
        existing_data = pd.DataFrame() 
    
    
    if not existing_data.empty:
        upload_data = detect_changes(new_data,existing_data)


    else:
        # All data is new if no existing data
        new_data['date_updated'] = pd.Timestamp.today().strftime('%Y-%m-%d')  # Add the date_updated column
        new_data['is_active'] = 'Active'
        upload_data = new_data
    
    # Only upload new or changed data
    if not upload_data.empty:
        
        load_data_to_postgres(upload_data,district)

        print(f"Uploaded {len(upload_data)} new/changed records for district {district}.")
    else:
        print(f"No new or changed data for district {district}.")

def real_estate_scraping_job(districts:list):
    for district in districts:
        base_url = f'https://dh.hu/elado-ingatlan/lakas-haz/budapest/budapest-{district}-kerulet/-/20-200-mFt'

        page_content = fetch_page_content(base_url)
        last_page_number = get_last_page_number(page_content)
        new_data = scrape_values(base_url,last_page_number)


        # Data Quality Check: Drop rows where missing values are present
        new_data.dropna(inplace=True)

        upload_to_postgres(new_data, district)  


with DAG(
    dag_id='real_estate_pipeline',
    default_args=default_args,
    description='A simple DAG to scrape real estate listings and upload the changed data to the Postgres data warehouse',
    schedule_interval='0 18 * * *',  # Run daily at 18:00
    catchup=False,
    dagrun_timeout= timedelta(seconds= 600)  # Only run the latest task,

) as dag:
    
    is_website_available = HttpSensor(
        task_id="is_website_available",
        method="GET",
        http_conn_id='website_conn',
        endpoint='/elado-ingatlan/lakas-haz/budapest',
        response_check=lambda response: "Eladó budapesti lakás, ház" in response.text,
        poke_interval= 5,
        timeout= 20
    )


  

    with TaskGroup(group_id='batch_processing_group') as batch_group:
        batch_size = 5
        batches = [range(i, min(i + batch_size, 24)) for i in range(1, 24, batch_size)]
            
            # Loop through batches and create tasks
        for i, batch in enumerate(batches):
            PythonOperator(
                task_id=f'process_batch_{i+1}',
                python_callable=real_estate_scraping_job,
                op_kwargs={'districts': list(batch)}
            )

    # Set task dependencies
dbt_run_task = BashOperator(
    task_id = "dbt_run",
    bash_command= "cd /home/karesz/Desktop/data-engineering-practice-main/real_estate_project/real_estate/real_estate_project && dbt run"
    
)

dbt_snapshot_task= BashOperator(
    task_id = "dbt_snapshot",
    bash_command= "cd /home/karesz/Desktop/data-engineering-practice-main/real_estate_project/real_estate/real_estate_project && dbt snapshot"
)

is_website_available >> batch_group >> dbt_run_task >> dbt_snapshot_task
