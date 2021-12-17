
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
from textblob import TextBlob
from textblob_fr import PatternTagger,PatternAnalyzer

sc = pyspark.SparkContext('local')
sc = SparkSession(sc)


def union_all(dfs):
    if len(dfs) > 1:
        return dfs[0].unionAll(union_all(dfs[1:]))
    else:
        return dfs[0]

def sentiment_extractor(row):
    row_dict = row.asDict()
    row_dict['sentiment'] = TextBlob(row['full_text'],pos_tagger=PatternTagger(),analyzer=PatternAnalyzer()).sentiment[0]
    newrow = Row(**row_dict)
    return newrow


if __name__=="__main__":
    list_of_dataframes = []
    for file in os.listdir(r'../data_candidat'):
        print(file)
        list_of_dataframes.append(sc.read.format("csv").option("sep", ",").option("multiline",True).option("header","true").option('encoding','ISO-8859-1').load(r'../data_candidat/'+file))
    union_df = union_all([list_of_dataframes[i] for i in range(len(list_of_dataframes))])
    union_df_rdd = union_df.rdd
    union_df_new = union_df_rdd.map(lambda row: sentiment_extractor(row))
    union_df = sc.createDataFrame(union_df_new)
    union_df.toPandas().to_csv(r'../final_data/final_data.csv',index=False)
