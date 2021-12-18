# ECE_Data_Pipeline_Project

# Introduction

Dans le cadre de notre projet pour le module **Data Pipeline** nous avons réalisé une étude portant sur des données politiques en vue des élections présidentielles 2022.

Le projet comporte 3 parties:
- Collecte des données.
- Traitement des données (mise en forme, nettoyage..).
- Orchestration et automatisation du Data Pipeline.


## Collecte des données

Cette première étape est indispensable pour alimenter le pipeline de données en inputs.

Nous avons mis en place une API moyennant la bibliothéque `flask_restful` consommant l'API `tweepy` ayant pour rôle de scraper les données à partir de twitter.

Cette API joue le rôle d'intermédiaire entre twitter et `Nifi` , notre gestionnaire de flux de données.

Le script de l'API est disponible sous le répertoire *"/twitter_scraping"*.

Ci dessous quelques illustrations des schémas réalisé avec `Nifi`.

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/third_capture.PNG)

Le premier process group `Twitter_batch_scraping` consomme l'API twitter créé précédemment pour chaque candidat des élections.

Le deuxième process group `2017_elections`concerne un dataset portant sur les élections présidentielles de 2017.

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/second_capture.PNG)

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/first_capturePNG.PNG)

Pour chaque électeur, une requête `GET` sera envoyée vers l'API de twitter. La réponse reçue sera divisée sur 500 `FlowFile`. 

Nous procédons par la suite à l'extraction des informations les plus pertinentes via chaque `FlowFile`.

Les `FlowFile` seront regroupés dans un ficher `CSV` qui sera envoyé vers un topic `Kafka`.
## Traitement des données (mise en forme, nettoyage..)

Une fois les données collectées, nous avons procédés à la création d'un dataset regroupant les outputs de nos différents batchs pour créer un fichier final pour chaque candidat.

Nous avons effectués plusieurs opérations moyennant principalement `Pyspark` sur les données collectés via twitter et ceux des élections présidentielles de 2017 tel que:

- Suppression des doublons.
- Traitement des symboles.
- Traitement des dates.
- Ajout de colonnes (Feature Engineering).
- Calcul d'agrégats.

##  Orchestration et automatisation du Data Pipeline.

Dans le but d'automatiser notre flot de données, nous avons utilisés deux DAGs sur Airflow:

- Le premier se déclenche chaque 30 minutes pour effectuer les traitements nécessaires à l'obtention et le traitement des données propres à chaque candidat.

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/Capture_of_dag.PNG)

- Le deuxième se déclenche une fois par jour pour regrouper l'ensemble des données acquises sur tous les candidats.

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/Capture_main_dag.PNG)

## Applications supplémentaires:

Nous avons pu réaliser à partir des données politiques collectées deux applications:

- Une analyse des sentiments (`Textblob`).
- Une carte interactive comportant les statistiques par département (A partir des données structurées collectées) disponible sous le repertoire `/Scripts/temp-plot.html`.

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/france_map.png)

- Un Word cloud avec les termes les plus récurrents sur twitter (A partir des données non structurées collectées).

![enter image description here](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/word_cloud.png)
