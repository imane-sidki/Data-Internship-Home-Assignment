from datetime import timedelta, datetime
import json
import pandas as pd
import re
import html
import os
import sqlite3
from airflow.decorators import dag, task
from airflow import DAG
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.operators.python_operator import PythonOperator


TABLES_CREATION_QUERY = """CREATE TABLE IF NOT EXISTS job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(225),
    industry VARCHAR(225),
    description TEXT,
    employment_type VARCHAR(125),
    date_posted DATE
);

CREATE TABLE IF NOT EXISTS company (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    NTEGER,
    name VARCHAR(225),
    link TEXT,
    FOREIGN KEY (REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS education (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    NTEGER,
    required_credential VARCHAR(225),
    FOREIGN KEY (REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS experience (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    NTEGER,
    months_of_experience INTEGER,
    seniority_level VARCHAR(25),
    FOREIGN KEY (REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS salary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    NTEGER,
    currency VARCHAR(3),
    min_value NUMERIC,
    max_value NUMERIC,
    unit VARCHAR(12),
    FOREIGN KEY (REFERENCES job(id)
);

CREATE TABLE IF NOT EXISTS location (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    NTEGER,
    country VARCHAR(60),
    locality VARCHAR(60),
    region VARCHAR(60),
    postal_code VARCHAR(25),
    street_address VARCHAR(225),
    latitude NUMERIC,
    longitude NUMERIC,
    FOREIGN KEY (REFERENCES job(id)
)
"""


def extract():
    """Extract data from jobs.csv."""
    df = pd.read_csv("source/jobs.csv")
    for index, row in df.iterrows():
        context_str = str(row["context"])
        with open(f"staging/extracted/{index}.txt", "w") as file:
            file.write(context_str)

def is_empty_json(json_data):
    return not bool(json_data) or all(value is None or (isinstance(value, (list, dict)) and not value) for value in json_data.values())


def clean_html_encoded_text(encoded_text):
    # Decode HTML-encoded text
    cleaned_text = html.unescape(encoded_text)

    return cleaned_text


def transform():
    """Clean and convert extracted elements to json."""
    for index in range(8261):
        with open(f"staging/extracted/{index}.txt", "r") as file:
            file_content = file.read()

        try:
            data = json.loads(file_content)
            print(index)
            print(data.keys())

            seniority_level = ""
            if file_content.strip() != '{}':
                cleaned_description = clean_html_encoded_text(data.get("description",{}))
                cleaned_industry = clean_html_encoded_text(data.get("industry",{}))


                cleaned_description = cleaned_description.replace('&nbsp;', '')
                # pattern = r"Seniority Level:\s*(\w+)-?\w*"

                # Use re.search to find the pattern in the text
                match = re.findall(r'Seniority Level:</strong>(.*?)</p>', cleaned_description)
                seniority_level = match[0] if match else ""

                # soup = BeautifulSoup(cleaned_description, 'html.parser')

                # # Get the text content without HTML tags
                # cleaned_description = soup.get_text(separator=' ', strip=True)

             # Initialize the transformed_data dictionary
            transformed_data = {"job": {}, "company": {}, "education": {}, "experience": {}, "salary": {}, "location": {}}

            # Extract job details
            transformed_data["job"]["title"] = data.get("title", "")
            transformed_data["job"]["industry"] = cleaned_industry
            transformed_data["job"]["description"] = cleaned_description
            transformed_data["job"]["employment_type"] = data.get("employmentType", "")
            transformed_data["job"]["date_posted"] = data.get("datePosted", "")

            # Extract company details
            company_data = data.get("hiringOrganization", {})
            transformed_data["company"]["name"] = company_data.get("name", "")
            transformed_data["company"]["link"] = company_data.get("sameAs", "")

            # Extract education details
            education_data = data.get("educationRequirements", {})
            transformed_data["education"]["required_credential"] = education_data.get("credentialCategory", "")

            experience_data = data.get("experienceRequirements", {})
            if isinstance(experience_data, dict):
                transformed_data["experience"]["months_of_experience"] = experience_data.get("monthsOfExperience", "")
                transformed_data["experience"]["seniority_level"] = seniority_level
            elif isinstance(experience_data, str):
                # Handle the case where "experienceRequirements" is a string
                transformed_data["experience"]["months_of_experience"] = ""
                transformed_data["experience"]["seniority_level"] = seniority_level
            else:
                transformed_data["experience"]["months_of_experience"] = ""
                transformed_data["experience"]["seniority_level"] = ""

            # Extract salary details
            salary_data = data.get("estimatedSalary", {})
            transformed_data["salary"]["currency"] = salary_data.get("currency", "")
            transformed_data["salary"]["min_value"] = salary_data.get("value", {}).get("minValue", "")
            transformed_data["salary"]["max_value"] = salary_data.get("value", {}).get("maxValue", "")
            transformed_data["salary"]["unit"] = salary_data.get("value", {}).get("unitText", "")

            # Extract location details
            location_data = data.get("jobLocation", {}).get("address", {})
            transformed_data["location"]["country"] = location_data.get("addressCountry", "")
            transformed_data["location"]["locality"] = location_data.get("addressLocality", "")
            transformed_data["location"]["region"] = location_data.get("addressRegion", "")
            transformed_data["location"]["postal_code"] = location_data.get("postalCode", "")
            transformed_data["location"]["street_address"] = location_data.get("streetAddress", "")
            transformed_data["location"]["latitude"] = data.get("jobLocation", {}).get("latitude", "")
            transformed_data["location"]["longitude"] = data.get("jobLocation", {}).get("longitude", "")

            with open(f"staging/transformed/{index}.json", "w") as file:
                json.dump(transformed_data, file, indent=2)

        except json.JSONDecodeError:
            if file_content.strip() == '{}' or 'nan' in file_content.lower():
                print(f"Skipping file {index} as it contains an empty JSON object or 'nan'.")
            else:
                print(f"Skipping file {index} due to invalid JSON.")
            continue

def load():
    """Load data to sqlite database."""
    sqlite_conn = sqlite3.connect('/usr/local/airflow/db/linkedin.db') 
    cursor = sqlite_conn.cursor()

    staging_folder = "staging/transformed/"
    i = 0
    for filename in os.listdir(staging_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(staging_folder, filename)

            with open(file_path, "r") as file:
                try:
                    data = json.load(file)
                    job_data = data.get("job", {})
                    job_insert_query = """
                        INSERT INTO job (title, industry, description, employment_type, date_posted)
                        VALUES (?, ?, ?, ?, ?)
                    """
                    cursor.execute(job_insert_query, (
                        job_data.get("title", ""),
                        job_data.get("industry", ""),
                        job_data.get("description", ""),
                        job_data.get("employment_type", ""),
                        job_data.get("date_posted", "")
                        ))
                   
                    company_data = data.get("company", {})
                    company_insert_query = """
                        INSERT INTO company (job_id, name, link) VALUES (?, ?, ?)
                    """
                    cursor.execute(company_insert_query, (
                        i,
                        company_data.get("name", ""),
                        company_data.get("link", "")
                        ))
                    
                    experience_data = data.get("experience", {})
                    experience_insert_query = """
                        INSERT INTO experience (job_id, months_of_experience, seniority_level)
                        VALUES (?, ?, ?)
                    """

                    cursor.execute(experience_insert_query, (
                        i,
                        experience_data.get("months_of_experience", 0),
                        experience_data.get("seniority_level", "")
                        ))
                    
                    education_data = data.get("education", {})
                    education_insert_query = """
                        INSERT INTO education (job_id, required_credential)
                        VALUES (?, ?)"""
                    
                    cursor.execute(education_insert_query, (
                        i,
                        education_data.get("required_credential", "")
                        ))
                    
                    salary_data = data.get("salary", {})
                    salary_insert_query = """
                        INSERT INTO salary (job_id, currency, min_value, max_value, unit)
                        VALUES (?, ?, ?, ?, ?)
                    """
                    cursor.execute(salary_insert_query, (
                        i,
                        salary_data.get("currency", ""),
                        salary_data.get("min_value", 0),
                        salary_data.get("max_value", 0),
                        salary_data.get("unit", "")
                        ))
                    
                    location_data = data.get("location", {})
                    location_insert_query = """
                        INSERT INTO location (job_id, country, locality, region, postal_code, street_address, latitude, longitude)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    cursor.execute(location_insert_query, (
                        i,
                        location_data.get("country", ""),
                        location_data.get("locality", ""),
                        location_data.get("region", ""),
                        location_data.get("postal_code", ""),
                        location_data.get("street_address", ""),
                        location_data.get("latitude", 0),
                        location_data.get("longitude", 0)
                        ))
                    sqlite_conn.commit()
                except json.JSONDecodeError:
                    print(f"Skipping file {filename} due to invalid JSON.")
                    continue
                except Exception as e:
                    print(f"Error processing file {filename}: {str(e)}")
                    continue
        i = i + 1

    
    sqlite_conn.close()


def load_data():
    conn = sqlite3.connect("/usr/local/airflow/db/linkedin.db")
    c = conn.cursor()
    try:

        c.execute("""
            CREATE TABLE IF NOT EXISTS job (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title VARCHAR(225),
            industry VARCHAR(225),
            description TEXT,
            employment_type VARCHAR(125),
            date_posted DATE);
        """)
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS company (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            name VARCHAR(225),
            link TEXT,
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
        """)
        
        c.execute("""
            CREATE TABLE IF NOT EXISTS education (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            required_credential VARCHAR(225),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS experience (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            months_of_experience INTEGER,
            seniority_level VARCHAR(25),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS salary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            currency VARCHAR(3),
            min_value NUMERIC,
            max_value NUMERIC,
            unit VARCHAR(12),
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
        """)

        c.execute("""
            CREATE TABLE IF NOT EXISTS location (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER,
            country VARCHAR(60),
            locality VARCHAR(60),
            region VARCHAR(60),
            postal_code VARCHAR(25),
            street_address VARCHAR(225),
            latitude NUMERIC,
            longitude NUMERIC,
            FOREIGN KEY (job_id) REFERENCES job(id)
        );
        """)
       
    except Exception as e:
        print(f"Error creating tables: {e}")
    finally:
        conn.commit()
        conn.close()


DAG_DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15)
}


dag = DAG(
    dag_id="etl_dag",
    description="ETL LinkedIn job posts",
    tags=["etl"],
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    default_args=DAG_DEFAULT_ARGS
)


extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

create_db = PythonOperator(
    task_id='create_db',
    python_callable=load_data,
    dag=dag,
)


create_db >> extract_task >> transform_task >> load_task 
