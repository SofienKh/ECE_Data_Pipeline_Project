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


##  Orchestration et automatisation du Data Pipeline.
