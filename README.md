# **Pipeline de Streaming de Données avec Spark, Kafka et Cassandra**

## **Description du Projet**

Ce projet implémente un pipeline de streaming de données qui utilise Apache Spark pour traiter des données en temps réel à partir d'Apache Kafka et les insérer dans une base de données Cassandra. Le pipeline est conçu pour la tolérance aux pannes et est capable de gérer des flux continus de données provenant de Kafka tout en garantissant que les données sont correctement transformées et stockées dans Cassandra.

## **Architecture**

L'architecture générale du projet se compose des composants suivants :

1. **Kafka** : Utilisé comme source de streaming. Les données sont envoyées à un topic Kafka (ex. : `users_created`) à partir d'une API qui génère des utilisateurs aléatoires.
   
2. **Spark** : Apache Spark est utilisé pour lire les données de Kafka, les transformer et les insérer dans Cassandra en continu. Spark est configuré pour être tolérant aux pannes avec la fonctionnalité de checkpointing.

3. **Cassandra** : Base de données NoSQL qui stocke les données transformées. Un keyspace et une table sont créés dynamiquement si nécessaire.

## **Composants Clés du Pipeline**

### 1. **Téléchargement et Formatage des Données**

Le pipeline commence par la récupération des données d'une API générant des utilisateurs aléatoires (ex. : [Random User API](https://randomuser.me/)) et le formatage des données pour les rendre compatibles avec le modèle de données attendu par Cassandra.

- **DoawloadDataTask** : Télécharge les données depuis l'API.
- **FormatDataTask** : Formate les données pour correspondre aux schémas de Cassandra.

### 2. **Envoi des Données à Kafka**

Les données formatées sont ensuite envoyées à un topic Kafka. 

- **KafkaDataPublisherTask** : Envoie les données formatées au topic `users_created` dans Kafka.

### 3. **Traitement des Données avec Spark**

Spark lit les messages provenant de Kafka, les transforme en DataFrame et applique le schéma défini pour structurer les données. Ensuite, il les écrit dans Cassandra en continu.

- **Spark** :
  - **create_spark_connection** : Crée une session Spark avec les connecteurs nécessaires pour Kafka et Cassandra.
  - **connect_to_kafka** : Lit les messages Kafka dans un DataFrame Spark.
  - **create_selection_df_from_kafka** : Applique un schéma pour structurer les données reçues.

### 4. **Stockage des Données dans Cassandra**

Les données transformées sont stockées dans une table Cassandra.

- **Cassandra** :
  - **create_cassandra_connection** : Établit une connexion avec le cluster Cassandra.
  - **create_keyspace** : Crée le keyspace `spark_streams` si nécessaire.
  - **create_table** : Crée la table `created_users` si elle n'existe pas déjà.
  - **insert_data** : Insère les données dans la table Cassandra à chaque micro-batch traité par Spark.

### 5. **Gestion des Checkpoints**

Le pipeline utilise la fonctionnalité de checkpointing de Spark pour assurer la tolérance aux pannes. Les checkpoints permettent à Spark de sauvegarder son état et de redémarrer là où il s'est arrêté en cas de panne ou de redémarrage du job.

- **Option `checkpointLocation`** : Spécifie le répertoire où les checkpoints sont stockés, permettant ainsi à Spark de gérer la continuité du flux en cas de panne.
                  
## **Usage**

### **Lancement du pipeline ETL**

Le pipeline se lance automatiquement via Airflow, en suivant ces étapes :

1. **Téléchargement des données** : Les données sont récupérées de l'API et formatées.
2. **Envoi à Kafka** : Les données formatées sont envoyées à Kafka.
3. **Streaming Spark** : Spark lit les données depuis Kafka, les transforme et les envoie à Cassandra.
4. **Stockage dans Cassandra** : Les données sont insérées dans la table `created_users`.


## **Prérequis**

Avant d'exécuter le projet, assurez-vous d'avoir les éléments suivants installés :

1. **Docker** et **Docker Compose**
2. **Apache Kafka** et **Zookeeper** (inclus dans la configuration Docker)
3. **Apache Spark** (via la configuration Docker)
4. **Apache Cassandra** (via Docker)

## **Installation**

1. Clonez le dépôt du projet :

```bash
git clone <url_du_projet>
cd <nom_du_dossier>


