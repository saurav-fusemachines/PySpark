from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf,col

spark = SparkSession.builder.appName("Netflix_Title").getOrCreate()

df = spark.read.json('/home/saurav/Desktop/SparkAssignment/netflix_titles.json')
# df.select(df['show_id']).show()

# Q1. How many PG-13 titles are there?
pg13_count = df.where(df.rating=='PG-13').count()
print("Total PG-13 Titles:",pg13_count)


# Q2. How many titles an actor or actress appeared in?
split_cast=df.select(df.cast).withColumn('cast',F.explode(F.split('cast',',')))

cast_count= split_cast.groupBy('cast').count().orderBy('count', ascending=False)# groupby 'cast' and count the number of times each ac

remove_empty_cast = cast_count.filter(cast_count.cast!='')

total_cast_app = remove_empty_cast.show()



# 3. How many titles has a director has filmed?
split_director = df.select(df.director).withColumn('director',F.explode(F.split('director',',')))

count_director = split_director.groupBy('director').count().orderBy('director',ascending=False)

remove_empty_count = count_director.filter(count_director.director !='')

total_director_count = remove_empty_count.show()



# 4. What content is available in different countries?
df.groupBy('country','listed_in').count().drop('count').show()


# 5. How many movies were released in 2008?
total_2008 = df.where((df.release_year==2008) &  (df.type  == "Movie")).count()
print("Total movies released in 2008 is:",total_2008)


# 6.List all the movies whose duration is greater than 100 mins ?
def duration_100(replace_min):
    return replace_min.replace('min','')

durationUDF = udf(duration_100)

filter_movie = df.filter(df['type']=='Movie').select(df['title'],df['duration'])

replacing_min = filter_movie.select(df['title'],durationUDF(filter_movie['duration']))
duration_100 =  replacing_min.where(replacing_min['duration_100(duration)']>100).show()



# 7.List movies played by “Kareena Kapoor” ?

split_cast=df.select(df.cast,df.title).withColumn('cast',F.explode(F.split('cast',',')))
kareena = split_cast.filter(split_cast['cast']=='Kareena Kapoor').show()