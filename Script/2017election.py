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

import plotly.express as px

from pyspark.sql import functions as F

sc = pyspark.SparkContext('local')
sc = SparkSession(sc)

df1 = sc.read.format("csv").option("header", "true").load("/home/sofienkhanfir/Bureau/Projet/nifi-dev/nifi-1.15.0/2017_data")

df1.show()

to_sum=df1.select('Code du département','Inscrits','Abstentions','Votants','Blancs','Nuls','Exprimés')
sum_exprs = {x: "sum" for x in to_sum.columns if x is not to_sum.columns[0]}
sum_df=to_sum.groupBy("Code du département").agg(sum_exprs)
sum_df=sum_df.withColumnRenamed("sum(Abstentions)", "Nombre d'abstentions").withColumnRenamed("sum(Exprimés)", "Nombre de votes exprimés").withColumnRenamed("sum(Blancs)", "Nombre de votes blancs") .withColumnRenamed("sum(Inscrits)", "Nombre d'inscrits") .withColumnRenamed("sum(Votants)", "Nombre de votants") .withColumnRenamed("sum(Nuls)", "Nombre de votes Nuls")

