from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import json

sc = SparkContext(appName='Test')
sc.setLogLevel('ERROR')

spark = SparkSession.builder.getOrCreate()

df = spark.read.json("/user/s2419440/filtered_tweets_*/*") 

df1 = df.selectExpr("`user.id` as user", "created_at as date")  

df2 = df1.select(df1.user, df1.date.substr(5,6).alias('date')) 

df3 = df2.groupBy(df2.user, df2.date).count() 

# unique users that tweeted per day
df4 = df3.groupBy(df3.date).count().withColumnRenamed("count", "number")

as_list = df4.collect()

d = {}
for l in as_list:
	date, count = l.date, l.number
	d[date] = count

json.dump(d, open('/home/s1866303/users_per_day.txt','w'))
	
