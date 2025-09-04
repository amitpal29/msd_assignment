from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from datetime import datetime
import subprocess
import json

# Read Airflow Variables (set these in Airflow UI or CLI)
SCRIPT_PATH = Variable.get("my_python_script")
DB_USER = Variable.get("DB_USER")
DB_PASSWORD = Variable.get("DB_PASSWORD")
DB_HOST = Variable.get("DB_HOST")
DB_NAME = Variable.get("DB_NAME")


api_url = "https://api.cnb.cz/cnbapi/fxrates/daily-month?lang=EN"
data_dir = "/mnt/c/Users/amitp/OneDrive/Desktop/ap-cnb-cx/data"
dbt_project = "/mnt/c/Users/amitp/OneDrive/Desktop/ap-cnb-cx"


# Define DAG
with DAG(
    dag_id="dag_fx_avg_data",
    start_date=datetime(2025, 8, 1),
    schedule="0 0 1 * *",
    params={
            "batch_dt": "default_value",
        },
    catchup=False,
) as dag:

    execute_script_task = BashOperator(
        task_id='load_fx_data_to_stage',
        bash_command = (
             f"python {SCRIPT_PATH}/get_data_api.py "
             f"--DB_USER {DB_USER} "
             f"--DB_PASSWORD {DB_PASSWORD} "
             f"--DB_HOST {DB_HOST} "
             f"--DB_NAME {DB_NAME} "
             f"--api_url {api_url} "
             f"--data_dir {data_dir} "
             f"--batch_dt {{{{ params.batch_dt }}}}"
        ),
        dag=dag,
    )

    run_dbt_model = BashOperator(
        task_id="cal_avg_fx_rates",
        bash_command=f"source {dbt_project}/venv/bin/activate && dbt run --project-dir /mnt/c/Users/amitp/OneDrive/Desktop/ap-cnb-cx \
          --profiles-dir /home/amitp/.dbt --select cur_fx_avg_rates --vars '{{{{ params | tojson }}}}'",
        params={
        "batch_dt": "default_value",
    },
    )

    execute_script_task >> run_dbt_model
