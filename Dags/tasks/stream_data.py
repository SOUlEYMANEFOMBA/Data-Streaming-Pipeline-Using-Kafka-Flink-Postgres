from kafka import KafkaProducer
import json
import logging
import time

class StreamingDataTask():
    """Cette task envoie des données formatées à Kafka"""
    def streaming_data(self, doawloaddata,formatData):
        logging.info(f"started data streaming into kafka")
        # producer=None
        producer = KafkaProducer(bootstrap_servers='broker:29092', max_block_ms=5000)
        curr_time =time.time()
        
        while True :
          if time.time() > curr_time + 60 :# 1 minutes 
                break
          try :
            res=doawloaddata.get_data()
            format_data=formatData.format_data(res)
            logging.info(f"Voici les données envoyer à kafka {format_data}")
            producer.send('users_created', json.dumps(format_data).encode('utf-8'))
            producer.flush()  # Pour s'assurer que le message est bien envoyé avant de fermer le producteur
            logging.info(f"Data sent successfully")
          except Exception as e:
            logging.error(f"Erreur lors de la production de données Kafka: {e}")
            continue
    
          # finally:
          #  if producer is not None:
          #    producer.close()
