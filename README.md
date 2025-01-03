# Project README

## Introduction

### Why this project?

When I started getting into data engineering, I wanted a hands-on project to solidify my skills. I decided to find a real-life problem where I could apply the concepts I was learning. This project involves end-to-end data processing for real estate listings. The workflow begins by scraping real estate listings in each district of Budapest from a website and ingesting the raw data into MinIO, an S3-compatible self-hosted object storage. The data is then transferred to PostgreSQL, which serves as the data warehouse for the project. Using dbt, the raw data is transformed and loaded into a refined gold layer (following the Medallion architecture), preparing it for analysis.

For data visualization, Apache Superset is used to create dashboards powered by the cleaned and structured data in the gold layer. Additionally, Apache Airflow is set up to automate the entire data pipeline, managing the orchestration and scheduling of tasks.

## Architecture and Setup of the Project

To get this project up and running on your local machine, follow the steps outlined below. This guide assumes you have basic familiarity with Docker and Python virtual environments.


![Flowchart Description](https://github.com/agostge/real-estate-project/real_estate/blob/main/assets/architecture.drawio.png)


### Prerequisites

1. **Docker**: Make sure Docker is installed on your machine. This project leverages Docker for services like MinIO, PostgreSQL, and pgAdmin, serving as the GUI for our data warehouse.
2. **Python**: Python 3.8+ is required for running dbt and other Python-related tasks.
3. **dbt**: dbt serves as our data transformation tool.
4. **Superset**: For data visualization, Superset can be installed locally via Python or Docker.
5. **Airflow**: The pipeline is automated to run daily, providing fresh data for the data warehouse. Airflow is used for orchestration.
6. **PostgreSQL**: To act as our data warehouse.
7. **MinIO**: Provides an S3-compatible open-source storage system.

### Setup Instructions

#### Step 1: Set up a Python Virtual Environment

First, create a Python virtual environment by running the following commands:

```bash
python3 -m venv <virtual_env_name>
source <virtual_env_name>/bin/activate
```

#### Step 2: Install Python Dependencies

Once the virtual environment is activated, install the required dependencies from the requirements.txt file:

```bash
Copy code
pip install -r requirements.txt
```

#### Step 3: Configure the Docker Containers

The docker-compose file is located in the config folder of the project, along with the necessary configuration files containing the environment variables.

To set up the Docker containers, follow these steps:

Navigate to the folder containing the docker-compose.yaml file:
```bash
Copy code
cd config
```

Run the following command to start the containers in detached mode:

```bash
Copy code
docker-compose up -d
```

This will bring up the services such as MinIO, PostgreSQL, Airflow, and Superset.

#### Step 4: Configure Environment Variables

Make sure you have configured all the necessary environment variables in the corresponding .env file(s). These variables will connect the services and configure them with the correct credentials and paths.

## Web Scraping

The data source in this project is a real estate website in Hungary. The search criteria are refined to include all listings from the 23 districts of Budapest. The data is scraped using the BeautifulSoup Python library, which parses the HTML of the website and extracts the relevant details such as property price, size, and location.

In addition to scraping, basic data quality checks are implemented to ensure that incomplete or corrupted listings are excluded from further processing. This helps ensure that only valid listings are ingested into the system for downstream processing and analysis.

### Scraping Process:
- **Request**: The scraping script sends HTTP requests to the real estate website.
- **Parsing**: The HTML content is parsed using BeautifulSoup, where relevant data points (e.g., price, area, address, etc.) are extracted.
- **Data Validation**: Basic data quality checks are applied to ensure that each listing has the necessary attributes like a valid price, size, and location.
- **Storage**: Cleaned data is then ingested into MinIO for object storage, awaiting transfer into PostgreSQL.

## Data Pipeline

Once the data is scraped and stored in MinIO, the pipeline begins.

### Data Transfer to PostgreSQL
The raw data from MinIO is transferred to PostgreSQL, acting as the project's central data warehouse. This data is stored in its raw form, ready to be cleaned and transformed.

### dbt Transformation
With dbt, the raw data is transformed using SQL models. The project follows the Medallion Architecture, where data moves through three layers:

- **Bronze Layer**: Raw data from MinIO is ingested into PostgreSQL.
- **Silver Layer**: Data is cleaned and enriched, including basic transformations such as data type conversions and the removal of duplicates.
- **Gold Layer**: The final, refined dataset is created, ready for analysis and visualization. This dataset is structured and optimized for use in dashboards and other reporting tools.

### Airflow for Orchestration
Airflow is used to automate the entire data pipeline, ensuring that each task runs on a schedule. Airflow handles:

- Scraping the real estate listings.
- Transferring data from MinIO to PostgreSQL.
- Running dbt transformations.
- Updating the dashboards.

Airflow's DAGs (Directed Acyclic Graphs) are defined to automate the daily execution of each pipeline task.

## Data Visualization with Superset

Apache Superset is used for data visualization. The gold layer data in PostgreSQL is used to create insightful dashboards, helping users explore the real estate data interactively.

### Setting up Superset
To set up Superset locally, you can follow these steps:
- **Run** Superset via Docker or Python.
- **Connect** to PostgreSQL as the data source.
- **Build Dashboards**: Create dashboards based on the structured data in the gold layer.
- **Explore the Data**: Use Superset's filtering and drill-down capabilities to explore the listings across different districts of Budapest.

## Conclusion

This project provides a complete end-to-end data pipeline for real estate listings, from web scraping to data transformation, storage, and visualization. By building this project, I gained hands-on experience with core data engineering concepts such as web scraping, data warehousing, ETL processes, and automation. The use of modern tools like dbt, Airflow, and Superset ensures that the pipeline is efficient, automated, and provides valuable insights into the real estate market in Budapest.
