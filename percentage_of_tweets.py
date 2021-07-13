from pyspark import SparkContext
from pyspark.sql import SparkSession
import json
from __future__ import division

sc = SparkContext(appName='Test')
sc.setLogLevel('ERROR')

spark = SparkSession.builder.getOrCreate()

df_all_russian = spark.read.json("/user/s1866303/all_russian_tweets/*") 

df_all_protest = spark.read.json("/user/s2419440/filtered_tweets_*/*")

all_per_day = df_all_russian.select(df_all_russian.created_at.substr(5,6).alias('date')).groupBy('date').count().withColumnRenamed("count", "number")

protests_per_day = df_all_protest.select(df_all_protest.created_at.substr(5,6).alias('date')).groupBy('date').count().withColumnRenamed("count", "number")

dates = []
for i in range(19,32):
	date = u'Jul '+str(i)
	dates.append(date)
	
for i in range(1,32):
	date = u'Aug {0:02d}'.format(i)
	dates.append(date)
	
all_list = all_per_day.collect() 
protest_list = protests_per_day.collect()

d_all = {}
for row in all_list:
	d_all[row.date] = row.number
	
d_prot = {}
for row in protest_list:
	d_prot[row.date] = row.number
	
d = {}
for date in d_all.keys():
	d[date] = d_prot[date] / d_all[date]


# dump results in json file to plot
json.dump(d, open('/home/s1866303/percentage_per_day.txt','w'))
	