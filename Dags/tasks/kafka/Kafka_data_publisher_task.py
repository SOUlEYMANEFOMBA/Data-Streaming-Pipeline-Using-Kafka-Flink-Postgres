from kafka import KafkaProducer
import json
import logging
import time

class KafkaDataPublisherTask:
    """
    Cette classe publie des données formatées dans un topic Kafka.

    Attributes:
        None
    """
    
    def streaming_data(self, doawloaddata, formatData):
        """
        Cette méthode envoie des données téléchargées et formatées au topic Kafka 'users_created' de manière continue
        pendant une durée limitée (ici 1 minute).

        Args:
            doawloaddata: Instance ou objet pour télécharger les données à partir d'une source.
            formatData: Instance ou objet pour formater les données dans le bon format avant l'envoi à Kafka.

        Returns:
            None

        Raises:
            Exception: En cas d'erreur pendant la production des données dans Kafka.
        """
        logging.info(f"Started data streaming into Kafka")

        # Initialise le producteur Kafka pour envoyer des messages au broker Kafka
        producer = KafkaProducer(bootstrap_servers='broker:29092', max_block_ms=5000)
        
        # Temps de départ pour la limite de 1 minute
        curr_time = time.time()
        
        # Envoie de données en continu pendant 1 minute
        while True:
            if time.time() > curr_time + 60:  # Arrête après 1 minute
                break
            
            try:
                # Télécharger les données à partir de la source spécifiée
                res = doawloaddata.get_data()
                
                # Formater les données avant de les envoyer à Kafka
                format_data = formatData.format_data(res)
                logging.info(f"Voici les données envoyées à Kafka: {format_data}")
                
                # Envoyer les données formatées au topic Kafka 'users_created'
                producer.send('users_created', json.dumps(format_data).encode('utf-8'))
                
                # S'assurer que les messages sont bien envoyés
                producer.flush()
                logging.info(f"Data sent successfully")
            
            except Exception as e:
                # Enregistrer les erreurs éventuelles pendant l'envoi
                logging.error(f"Erreur lors de la production de données Kafka: {e}")
                continue
