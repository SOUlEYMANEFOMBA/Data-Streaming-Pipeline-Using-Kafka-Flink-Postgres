from kafka import KafkaProducer
import json

class StreamingDataTask():
    """Cette task envoie des données formatées à Kafka"""
    def streaming_data(self, doawloaddata,formatData):
        producer=None
        res=doawloaddata.get_data()
        formatData=formatData.format_data(res)
        print(formatData)
        try:
            producer = KafkaProducer(bootstrap_servers='localhost:9092', max_block_ms=5000)
            producer.send('users_created', json.dumps(formatData).encode('utf-8'))
            producer.flush()  # Pour s'assurer que le message est bien envoyé avant de fermer le producteur
            print("Data sent successfully")
        except Exception as e:
          print(f"Erreur lors de la production de données Kafka: {e}")
    
        finally:
          if producer is not None:
            producer.close()
