Introduction

Why this project ? When I started getting into data engineering in 2024, I wanted a hands-on project, and I decided to find a real-life problem, where I could practice my skills. 
This project involves end-to-end data processing for real estate listings. The workflow starts by scraping real estate listings in in each district of Budapest from a website and ingesting the raw data into MinIO , an S3-compatible self-hosted object storage. The data is then transferred to PostgreSQL, which serves as the Data warehouse for the project.  Using dbt, the raw data is transformed and loaded into a refined gold layer (following the Medallion architecture), preparing it for analysis.

For data visualization, Superset is used to create dashboards, powered by the cleaned and structured data in the gold layer. Additionally, Airflow is set up to automate the entire data pipeline, managing the orchestration and scheduling of tasks.


Architecture and setup of the project

To get this project up and running on your local machine, follow the steps outlined below. This guide assumes you have basic familiarity with Docker and Python virtual environments.

Prerequisites
Docker: Make sure Docker is installed on your machine. This project leverages Docker for services like MinIO, PostgreSQL, and pgAdmin, serving as GUI for our data warehouse.
Python:  Python 3.8+ for running dbt and other Python-related tasks.
dbt: dbt serves as our data transformation tool.
Superset: For data visualization, Superset can be installed locally via Python or Docker.
Airflow: The pipeline is automated to run daily, providing fresh data for the data warehouse. Airflow is used for orchestration.
Postgres: To act as our warehouse
Minio: To provide an S3 compatible open source storage system. 

First of all, a python virtual environment will be needed, which can be et up with the following command:

python3 -m venv <virtual_env_name>
source <virtual_env_name>/bin/activate

# Install the Python dependencies
pip install -r requirements.txt

The docker-compose file is situated in the config folder of the project, along with the config files containing the environment variables for the project

Once the environment variables are set up, the docker container can be set up by navigating to the folder containing the docker-copose.yaml file and running the following command:

docker-compose up -d





Web scraping

The data source in my project is a real estate website in Hungary, where search criteria is refined to include all listings from the 23 districts of Budapest. The BeautifulSoup python library was used to achieve the scraping, while basic data quality checks were implemented to exclude incomplete data