import csv
from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from snowflake import connector as snow
from airflow.hooks.base_hook import BaseHook
import re


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_data_from_api(**kwargs):
    api_endpoint = "https://www.arbeitnow.com/api/job-board-api"
    response = requests.get(api_endpoint)
    if response.status_code == 200:
        return response.json()["data"]
    else:
        raise Exception(f"Failed to fetch data from API. Status code: {response.status_code}")

def store_data_in_snowflake(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data_from_api')

    snowflake_conn_id = 'snowflake-conn'
    snowflake_hook = BaseHook.get_hook(snowflake_conn_id)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        pattern = re.compile(r'\s*\([^()]*m\/w\/d[^()]*\)\s*|\s*\([^()]+\)\s*|\s*m\/w\/d\s*')

        for item in data:
            if isinstance(item, dict):

                tags_str = str(item["tags"]).replace("'", '"')
                job_types_str = str(item["job_types"]).replace("'", '"')

                title = pattern.sub('', item["title"])  

                cursor.execute("""
                    INSERT INTO JOBS (slug, company_name, title, description, remote, url, tags, job_types, location, created_at)
                    SELECT %s, %s, %s, %s, %s, %s, PARSE_JSON(%s), PARSE_JSON(%s), %s, %s
                """, (
                    item["slug"],
                    item["company_name"],
                    title,  
                    item["description"],
                    item["remote"],  
                    item["url"],
                    tags_str,
                    job_types_str,
                    item["location"],
                    datetime.fromtimestamp(item["created_at"])  
                ))
            else:
                print("Warning: Item is not a dictionary")

        conn.commit()
    except Exception as e:
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()


def suggest_courses_for_jobs(**kwargs):
    snowflake_conn_id = 'snowflake-conn'
    snowflake_hook = BaseHook.get_hook(snowflake_conn_id)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    try:
        pattern = re.compile(r'\s*\([^()]*m\/w\/d[^()]*\)\s*|\s*\([^()]+\)\s*|\s*m\/w\/d\s*')

        cursor.execute("""
            INSERT INTO SUGGESTED_COURSES (job_title, course_title)
            SELECT DISTINCT 
                REGEXP_REPLACE(j.TITLE, %s, ''),
                c."Course Title"
            FROM JOBS j
            JOIN COURSES c
            ON (LOWER(j.TITLE) IN (LOWER(c."Course Title"), LOWER(j.DESCRIPTION), LOWER(c."What you will learn"), LOWER(c."Skill gain"))
                OR LOWER(j.DESCRIPTION) IN (LOWER(c."Course Title"), LOWER(j.TITLE), LOWER(c."What you will learn"), LOWER(c."Skill gain"))
                OR LOWER(c."Course Title") IN (LOWER(j.TITLE), LOWER(j.DESCRIPTION), LOWER(c."What you will learn"), LOWER(c."Skill gain"))
                OR LOWER(c."What you will learn") IN (LOWER(j.TITLE), LOWER(j.DESCRIPTION), LOWER(c."Course Title"), LOWER(c."Skill gain"))
                OR LOWER(c."Skill gain") IN (LOWER(j.TITLE), LOWER(j.DESCRIPTION), LOWER(c."Course Title"), LOWER(c."What you will learn")))
        """, (pattern.pattern,))

        conn.commit()
    except Exception as e:
        print("Error:", e)
    finally:
        cursor.close()
        conn.close()


with DAG(
    'store_api_data_in_snowflake',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 3, 19),
    catchup=False
) as dag:
    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_api',
        python_callable=fetch_data_from_api,
        provide_context=True,
    )

    store_data_task = PythonOperator(
        task_id='store_data_in_snowflake',
        python_callable=store_data_in_snowflake,
        provide_context=True,
    )

    suggest_courses_task = PythonOperator(
        task_id='suggest_courses_for_jobs',
        python_callable=suggest_courses_for_jobs,
        provide_context=True,
    )

    fetch_data_task >> store_data_task >> suggest_courses_task
