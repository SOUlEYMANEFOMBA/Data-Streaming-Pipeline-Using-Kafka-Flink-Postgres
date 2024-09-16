from Dags.tasks.kafka.doawload_data_task import DoawloadDataTask
from Dags.tasks.kafka.format_data_task import FormatDataTask
from Dags.tasks.kafka.Kafka_data_publisher_task import KafkaDataPublisherTask

def main():
    doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
    formatData =FormatDataTask()
    stream=KafkaDataPublisherTask()
    stream.streaming_data(doawloaddata,formatData)

if __name__=="__main__":
    main()