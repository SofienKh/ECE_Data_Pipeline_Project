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


sc = pyspark.SparkContext('local')
sc = SparkSession(sc)

list_of_dataframes = []
candidat = "Jean-luc Mélenchon"

def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]
def date_parser(row):
    row_dict = row.asDict()
    row_dict['Date'] = datetime.strptime(row_dict["created_at"], '%b %d %H:%M:%S %Y')
    newrow = Row(**row_dict)
    return newrow
def clean_dataframe(dataframe):
    dataframe = dataframe.dropDuplicates(dataframe.columns)
    dataframe  = dataframe.na.drop(subset=["created_at","full_text"])
    dataframe = dataframe.withColumn('full_text', f.regexp_replace(f.col("full_text"), r'[^A-Za-z0-9\s]+', ""))
    dataframe = dataframe.withColumn('user_name', f.regexp_replace(f.col("user_name"), r'[^A-Za-z0-9\s]+', ""))
    dataframe = dataframe.withColumn('user_location', f.regexp_replace(f.col("user_location"), r'[^A-Za-z0-9\s]+', ""))
#    dataframe=dataframe.withColumn('created_at', substring('created_at', 5,50))
#    dataframe=dataframe.withColumn('created_at', f.regexp_replace(f.col("created_at"), r'\+[0-9]*', ""))
#    dataframe_rdd = dataframe.rdd
#    dataframe_new = dataframe_rdd.map(lambda row: date_parser(row))
#    dataframe = sc.createDataFrame(dataframe_new)
    return dataframe

if __name__ =="__main__":
    for file in os.listdir(r'../nifi-dev/data_candidat_2022/'+candidat):
        list_of_dataframes.append(sc.read.format("csv").option("sep", ",").option("multiline",True).option("header","true").option('encoding','ISO-8859-1').load(r'../nifi-dev/data_candidat_2022/'+candidat+'/'+file).select(["id","created_at","user_name","user_location","full_text","user_followers_count","retweet_count"]))
    union_df = union_all([list_of_dataframes[i] for i in range(len(list_of_dataframes))])
    union_df = clean_dataframe(union_df)
    union_df = union_df.withColumn("Candidat", lit(candidat))
    union_df.toPandas().to_csv(r'../data_candidat/'+candidat+'.csv',index=False)



