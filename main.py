from Dags.tasks.kafka.doawload_data_task import DoawloadDataTask
from Dags.tasks.kafka.format_data_task import FormatDataTask
from Dags.tasks.kafka.Kafka_data_publisher_task import KafkaDataPublisherTask
from Dags.tasks.cassandra.create_cassandra_connection_task import CreateCassandraConnectionTask
from Dags.tasks.spark.spark_streaming_task import SparkStreamingTask
import logging

def main():
    # doawloaddata = DoawloadDataTask('https://randomuser.me/api/')
    # formatData =FormatDataTask()
    # stream=KafkaDataPublisherTask()
    # stream.streaming_data(doawloaddata,formatData)
    spark_task = SparkStreamingTask()
    cassandra_task = CreateCassandraConnectionTask()
    # create spark connection
    spark_conn = spark_task.create_spark_connection()  ##Crée une session Spark configurée pour interagir avec Cassandra et Kafka

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = spark_task.connect_to_kafka(spark_conn)  ##Crée un DataFrame Spark en consommant les données depuis Kafka.
        selection_df = spark_task.create_selection_df_from_kafka(spark_df) ##Sélectionne et transforme les données JSON depuis Kafka en colonnes individuelles dans un DataFrame
        session = cassandra_task.create_cassandra_connection() ##Crée une connexion à un cluster Cassandra.

        if session is not None:
            cassandra_task.create_keyspace(session) ##Crée un keyspace(une base de donnée) dans Cassandra si ce n'est pas déjà fait.
            cassandra_task.create_table(session)  ##Crée une table dans le keyspace Cassandra si elle n'existe pas déjà
 
            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream
                               .format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())

            streaming_query.awaitTermination()

if __name__=="__main__":
    main()