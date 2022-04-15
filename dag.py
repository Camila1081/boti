import datetime

import airflow
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from etapa_raw import etapa_raw
from etapa_staged import etapa_staged
from etapa_bigquery import etapa_bigquery
import google.auth


credentials, project_id = google.auth.default()


# If you are running Airflow in more than one time zone
# see https://airflow.apache.org/docs/apache-airflow/stable/timezone.html
# for best practices
YESTERDAY = datetime.datetime.now() - datetime.timedelta(days=1)

default_args = {
    'owner': 'Composer Example',
    'depends_on_past': False,
    'email': [''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'start_date': YESTERDAY,
}

with airflow.DAG(
        'composer_sample_dag',
        catchup=False,
        default_args=default_args,
        schedule_interval=datetime.timedelta(days=1)) as dag:
        
    bucket_name_raw = "base-raw"
    bucket_name_staged = "base-staged"
    project_name = 'boti-347200'

    etapa_ingestao = PythonOperator(
            task_id='ingestao',
            python_callable=etapa_raw,
            op_kwargs={'bucket_name_raw':'base-raw','bucket_name_staged':'base-staged'},
            dag=dag,
            
    )
    etapa_transformacao = PythonOperator(
            task_id='transformacao',
            python_callable=etapa_staged,
            op_kwargs={'bucket_name_staged' : "base-staged",'project_name': 'boti-347200'},
            dag=dag
    )

    etapa_db = PythonOperator(
            task_id='db',
            python_callable=etapa_bigquery,
            op_kwargs={'bucket_name_staged' : "base-staged",'project_name': 'boti-347200'},
            dag=dag
    )
    etapa_ingestao>>etapa_transformacao>>etapa_db
 