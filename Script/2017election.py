import findspark
findspark.init()
import pyspark
import random
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
import os
import pyspark.sql.functions as f
from pyspark.sql.functions import lit
import math
from pyspark.sql import Row
from pyspark.sql.functions import unix_timestamp, from_unixtime,substring
from datetime import datetime
import geopandas as gpd
import plotly
import plotly.express as px

from pyspark.sql import functions as F

sc = pyspark.SparkContext('local')
sc = SparkSession(sc)

df1 = sc.read.format("csv").option("header", "true").load("/home/sofienkhanfir/Bureau/Projet/nifi-dev/nifi-1.15.0/2017_data")

to_sum=df1.select('Code du département','Inscrits','Abstentions','Votants','Blancs','Nuls','Exprimés')
sum_exprs = {x: "sum" for x in to_sum.columns if x is not to_sum.columns[0]}
sum_df=to_sum.groupBy("Code du département").agg(sum_exprs)
sum_df=sum_df.withColumnRenamed("sum(Abstentions)", "Nombre d'abstentions").withColumnRenamed("sum(Exprimés)", "Nombre de votes exprimés").withColumnRenamed("sum(Blancs)", "Nombre de votes blancs") .withColumnRenamed("sum(Inscrits)", "Nombre d'inscrits") .withColumnRenamed("sum(Votants)", "Nombre de votants") .withColumnRenamed("sum(Nuls)", "Nombre de votes Nuls")

gdf = gpd.read_file('https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements-avec-outre-mer.geojson')
gdf=gdf.replace({'code' : { '01' : '1', '02' : "2", '03' : "3", '04' : "4", '05' : "5", '06' : "6", '07' : "7", '08' : "8", '09' : "9"}})
geo_df = gdf.merge(sum_df.toPandas(), left_on="code",right_on="Code du département",how="left").fillna(0)

fig = px.choropleth_mapbox(geo_df,
                           geojson=geo_df.geometry,
                           locations=geo_df["code"],
                           center={"lat": 46.5, "lon": 2.19},
                           mapbox_style="carto-darkmatter",
                           hover_data=["Nombre d'abstentions","Nombre de votes exprimés","Nombre de votes blancs","Nombre d'inscrits","Nombre de votants","Nombre de votes Nuls"],
                           zoom=4.4,
                           width=1800,
                           height=600)

plotly.offline.plot(fig)

df1.toPandas().to_csv("../final_data/presidentiel2017.csv",index=False)
