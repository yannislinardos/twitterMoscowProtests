from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
import json

sc = SparkContext(appName='Test')
sc.setLogLevel('ERROR')

spark = SparkSession.builder.getOrCreate()

df = spark.read.json(["/user/s2419440/filtered_tweets_*/*", "/user/s1866303/filtered_tweets19_25/*"]) 

#all_data_df = spark.read.json("/user/s1866303/twitter_data/*/*/*/*.json.bz2")

#df = all_data_df.where(all_data_df.lang == u'ru')

hashtags = df.select(df.hashtags ,df.created_at)  
exploded_hashtags = hashtags.select(explode(hashtags.hashtags).alias('hashtag'), hashtags.created_at.substr(5,6).alias('date'))    

hashtagsByCount = exploded_hashtags.groupby(exploded_hashtags.hashtag).count().sort('count', ascending=False)

hashtagsByDate = exploded_hashtags.groupby(exploded_hashtags.date, exploded_hashtags.hashtag).count().withColumnRenamed("count", "number")
sorted_hashtags_per_day = hashtagsByDate.orderBy(hashtagsByDate.date, hashtagsByDate.number, ascending=False) 

popular_hashtags = [u'допускай', u'митинг', u'MoscowProtests', u'митингЗАсвободу', u'протесты']

search_expr = '|'.join(popular_hashtags) 

most_popular = sorted_hashtags_per_day.filter(sorted_hashtags_per_day.hashtag.rlike(search_expr)) 

in_list_form = most_popular.collect()  
 

dates = []
# for July
for i in range(19,32):
	date = u'Jul '+str(i)
	dates.append(date)
	
# for August
for i in range(1,32):
	date = u'Aug {0:02d}'.format(i)
	dates.append(date)
	
	
dictionary = {}
for date in dates:
	dictionary[date] = []
for row in in_list_form: 
	dictionary[row.date].append((row.hashtag, row.number))
	
# dump results in json file to plot	
json.dump(dictionary, open('/home/s1866303/most_popular_hashtags.txt','w'))
	
	
	
