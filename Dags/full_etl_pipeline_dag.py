from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.kafka.doawload_data_task import DoawloadDataTask
from tasks.kafka.format_data_task import FormatDataTask
from tasks.kafka.Kafka_data_publisher_task import KafkaDataPublisherTask

default_args= {
    'ouner':'Fomba souleymane',
    'start_date':datetime(2024,9,12, 10,00),
    'retries': 1,
    'retry_delay':timedelta(minutes=5)
}
doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
formatData =FormatDataTask()
stream=KafkaDataPublisherTask()
# stream.streaming_data(doawloaddata,formatData)
with DAG('user_automate',
         default_args=default_args,
         schedule_interval=timedelta(days=1)) as dag :
    streaming_task= PythonOperator(
        task_id='streaming_data_from_user_random_api',
        python_callable=stream.streaming_data,
        op_args=[doawloaddata,formatData]
    )
