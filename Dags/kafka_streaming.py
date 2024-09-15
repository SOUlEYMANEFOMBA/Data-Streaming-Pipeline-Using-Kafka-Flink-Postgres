from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.doawload_data import DoawloadDataTask
from tasks.format_data import FormatDataTask
from tasks.stream_data import StreamingDataTask

default_args= {
    'ouner':'Fomba souleymane',
    'start_date':datetime(2024,9,12, 10,00),
    'retries': 1,
    'retry_delay':timedelta(minutes=5)
}
doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
formatData =FormatDataTask()
stream=StreamingDataTask()
# stream.streaming_data(doawloaddata,formatData)
with DAG('user_automate',
         default_args=default_args,
         schedule_interval=timedelta(days=1)) as dag :
    streaming_task= PythonOperator(
        task_id='streaming_data_from_user_random_api',
        python_callable=stream.streaming_data,
        op_args=[doawloaddata,formatData]
    )
