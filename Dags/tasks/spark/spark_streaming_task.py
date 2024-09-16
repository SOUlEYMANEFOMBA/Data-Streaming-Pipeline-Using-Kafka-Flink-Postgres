import logging

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType


def create_keyspace(session):
   pass


def create_table(session):
   pass


def insert_data(session, **kwargs):
   pass


def create_spark_connection():
    pass


def connect_to_kafka(spark_conn):
   pass


def create_cassandra_connection():
    pass


def create_selection_df_from_kafka(spark_df):
    pass


# if __name__ == "__main__":
#     # create spark connection
#     spark_conn = create_spark_connection()

#     if spark_conn is not None:
#         # connect to kafka with spark connection
#         spark_df = connect_to_kafka(spark_conn)
#         selection_df = create_selection_df_from_kafka(spark_df)
#         session = create_cassandra_connection()

#         if session is not None:
#             create_keyspace(session)
#             create_table(session)

#             logging.info("Streaming is being started...")

#             streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
#                                .option('checkpointLocation', '/tmp/checkpoint')
#                                .option('keyspace', 'spark_streams')
#                                .option('table', 'created_users')
#                                .start())

#             streaming_query.awaitTermination()