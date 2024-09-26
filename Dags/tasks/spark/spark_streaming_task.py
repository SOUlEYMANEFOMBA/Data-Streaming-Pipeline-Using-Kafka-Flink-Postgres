import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

class SparkStreamingTask():
    """
    Classe pour gérer les tâches de streaming avec Spark, Kafka et Cassandra.
    """

    def insert_data(self, session, **kwargs):
        """
        Insère les données dans Cassandra à partir des arguments passés.

        :param session: La session Cassandra
        :param kwargs: Les données utilisateur sous forme de dictionnaire avec les clés suivantes :
                       'id', 'first_name', 'last_name', 'gender', 'address', 'post_code', 
                       'email', 'username', 'dob', 'registered_date', 'phone', 'picture'
        """
        logging.info("Beginning of data insertion...")
        user_id = kwargs.get('id')
        first_name = kwargs.get('first_name')
        last_name = kwargs.get('last_name')
        gender = kwargs.get('gender')
        address = kwargs.get('address')
        postcode = kwargs.get('post_code')
        email = kwargs.get('email')
        username = kwargs.get('username')
        dob = kwargs.get('dob')
        registered_date = kwargs.get('registered_date')
        phone = kwargs.get('phone')
        picture = kwargs.get('picture')

        try:
            session.execute("""
                INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (user_id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture))
            logging.info(f"Data inserted for {first_name} {last_name}")
        except Exception as e:
            logging.error(f"Could not insert data due to {e}")

    def create_spark_connection(self):
        """
        Crée une session Spark configurée pour interagir avec Cassandra et Kafka.

        :return: L'objet SparkSession, ou None en cas d'erreur
        """
        logging.info("Beginning of Spark connection creation...")
        spark_connect = None
        try:
            spark_connect = SparkSession.builder \
                .appName('SparkDataStreaming') \
                .master('spark://spark-master:7077') \
                .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                               "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
                .config('spark.cassandra.connection.host', 'cassandra') \
                .getOrCreate()
            spark_connect.sparkContext.setLogLevel("ERROR")
            logging.info("Spark connection created successfully!")
        except Exception as e:
            logging.error(f"Couldn't create the Spark session due to exception: {e}")

        return spark_connect

    def connect_to_kafka(self, spark_connect):
        """
        Crée un DataFrame Spark en consommant les données depuis Kafka.

        :param spark_connect: La session Spark active
        :return: Un DataFrame Spark, ou None en cas d'erreur
        """
        logging.info("Beginning of Kafka DataFrame creation...")
        spark_df = None
        try:
            spark_df = spark_connect.readStream \
                .format('kafka') \
                .option('kafka.bootstrap.servers', 'broker:29092') \
                .option('subscribe', 'users_created') \
                .option('startingOffsets', 'earliest') \
                .load()
            logging.info("Kafka DataFrame created successfully!")
        except Exception as e:
            logging.warning(f"Kafka DataFrame could not be created due to: {e}")

        return spark_df

    def create_selection_df_from_kafka(self, spark_df):
        """
        Sélectionne et transforme les données JSON depuis Kafka en colonnes individuelles dans un DataFrame.

        :param spark_df: Le DataFrame Spark contenant les données brutes de Kafka
        :return: Un DataFrame Spark avec les colonnes des données utilisateur
        """
        logging.info("Beginning of Kafka DataFrame transformation...")
        schema = StructType([
            StructField("id", StringType(), False),
            StructField("first_name", StringType(), False),
            StructField("last_name", StringType(), False),
            StructField("gender", StringType(), False),
            StructField("address", StringType(), False),
            StructField("post_code", StringType(), False),
            StructField("email", StringType(), False),
            StructField("username", StringType(), False),
            StructField("registered_date", StringType(), False),
            StructField("phone", StringType(), False),
            StructField("picture", StringType(), False)
        ])

        try:
            sel = spark_df.selectExpr("CAST(value AS STRING)") \
                          .select(from_json(col('value'), schema).alias('data')) \
                          .select("data.*")
            logging.info("DataFrame transformed successfully!")
            print(sel)
        except Exception as e:
            logging.error(f"Could not transform Kafka DataFrame due to: {e}")
            return None

        return sel