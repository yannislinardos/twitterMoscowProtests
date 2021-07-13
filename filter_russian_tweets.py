from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext(appName='Test')
sc.setLogLevel('ERROR')

spark = SparkSession.builder.getOrCreate()

all_data_df = spark.read.json("/user/s1866303/twitter_data/*/*/*/*.json.bz2")

df = all_data_df.where(all_data_df.lang == u'ru')

df.write.json('/user/s1866303/all_russian_tweets')
