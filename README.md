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
![Nifi Schema](https://raw.githubusercontent.com/SofienKh/ECE_Data_Pipeline_Project/main/src/first_capturePNG.PNG)

## Traitement des données (mise en forme, nettoyage..)


##  Orchestration et automatisation du Data Pipeline.
