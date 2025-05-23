from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag
import boto3
import pendulum


AWS_ACCESS_KEY_ID = ""
AWS_SECRET_ACCESS_KEY = ""


def fetch_s3_file(bucket: str, key: str) -> str:
    session = boto3.session.Session()
    s3_client = session.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f'/data/{key}'
    )


bash_command_tmpl = """
for file in {{ params.files }}; do
  echo "Content of $file:"
  head -n 10 $file
done
"""


@dag(schedule_interval=None, start_date=pendulum.parse('2022-07-13'))
def sprint6_dag_get_data():
    bucket_files = ('dialogs.csv', 'groups.csv', 'users.csv', 'group_log.csv')
    fetch_tasks = [
        PythonOperator(
            task_id=f'fetch_{key}',
            python_callable=fetch_s3_file,
            op_kwargs={'bucket': 'sprint6', 'key': key},
        ) for key in bucket_files
    ]

    print_10_lines_of_each = BashOperator(
        task_id='print_10_lines_of_each',
        bash_command=bash_command_tmpl,
        params={'files': " ".join(f'/data/{f}' for f in bucket_files)}
    )

    begin = DummyOperator(task_id="begin")

    begin >> fetch_tasks >> print_10_lines_of_each


_ = sprint6_dag_get_data()
