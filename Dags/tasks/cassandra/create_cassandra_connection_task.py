import logging
from cassandra.cluster import Cluster

class CreateCassandraConnectionTask():
    """
    Tâche pour gérer la connexion et la configuration de Cassandra.
    
    """
    def create_keyspace(self,session):
        """
        Crée un keyspace dans Cassandra si ce n'est pas déjà fait.
        
        Args:
            session: La session Cassandra à utiliser pour exécuter les requêtes.
        """
        logging.info(f"Begginning of create Keyspace")
        session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS spark_streams
                        WITH REPLICATION= {
                            'class' : 'SimpleStrategy',
                            'replication_factor': '1'
                        };
                        """)
        logging.info(f"Keyspace created successfully!")


    def create_table(self,session):
        """
        Crée une table dans le keyspace Cassandra si elle n'existe pas déjà.
        
        Args:
            session: La session Cassandra à utiliser pour exécuter les requêtes.
        """
        logging.info(f"Beggining of create Table")
        session.execute("""
                        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
                        id UUID PRIMARY KEY,
                        first_name TEXT,
                        last_name TEXT,
                        gender TEXT,
                        address TEXT,
                        post_code TEXT,
                        email TEXT,
                        username TEXT,
                        registered_date TEXT,
                        phone TEXT,
                        picture TEXT);
                    """)
        logging.info(f"Table created successfully!")
    
    def create_cassandra_connection(self):
        """
        Crée une connexion à un cluster Cassandra.
        
        Returns:
            Session: La session Cassandra si la connexion est réussie, sinon None.
        """
        logging.info("Begginning of create Cassandra connection")
        try :
            #Connection to cassandra connector cluster
            cluster =Cluster(['cassandra'])
            cassandra_session=cluster.connect()
            logging.info("Cassandra connection created successfully!")
            return cassandra_session
        except Exception as e :
            logging.error(f"Could not create cassandra connection due to {e}"
                        )
            return None
            
