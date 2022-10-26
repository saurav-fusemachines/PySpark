from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf,col

spark = SparkSession.builder.appName("Netflix_Title").getOrCreate()

df = spark.read.json('/home/saurav/Desktop/SparkAssignment/netflix_titles.json')
# df.select(df['show_id']).show()

# Q1. How many PG-13 titles are there?
pg13_count = df.where(df.rating=='PG-13')
pg13_count.show()
print("Total PG-13 Titles:",pg13_count.count())

     #OUTPUT IN CSV
pg13_count.repartition(1).write.csv('PG-13',header=True, mode='overwrite')

     #OP Saved in Postgresql
pg13_count.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='PG_13', user='fusemachines', password='hello123').mode('overwrite').save()



# Q2. How many titles an actor or actress appeared in?
split_cast=df.select(df.cast).withColumn('cast',F.explode(F.split('cast',',')))

cast_count= split_cast.groupBy('cast').count().orderBy('count', ascending=False)# groupby 'cast' and count the number of times each ac

remove_empty_cast = cast_count.filter(cast_count.cast!='')

total_cast_app = remove_empty_cast

total_cast_app.show(10)

    #OUTPUT IN CSV
total_cast_app.repartition(1).write.csv('Total_Cast_apperance',header=True, mode='overwrite')

    #OP Saved in Postgresql
total_cast_app.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Actor_apperance', user='fusemachines', password='hello123').mode('overwrite').save()


# 3. How many titles has a director has filmed?
split_director = df.select(df.director).withColumn('director',F.explode(F.split('director',',')))

count_director = split_director.groupBy('director').count().orderBy('director',ascending=False)

remove_empty_count = count_director.filter(count_director.director !='')

total_director_count = remove_empty_count
total_director_count.show()

        #OUTPUT IN CSV
total_director_count.repartition(1).write.csv('Total_Director_Count',header=True, mode='overwrite')

    #OP Saved in Postgresql
total_director_count.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Total_Director_Count', user='fusemachines', password='hello123').mode('overwrite').save()


# 4. What content is available in different countries?
content=df.groupBy('country','listed_in').count().drop('count')
content.show()

      #OUTPUT IN CSV
content.repartition(1).write.csv('Content',header=True, mode='overwrite')

      #OP Saved in Postgresql
content.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Content_according_country', user='fusemachines', password='hello123').mode('overwrite').save()


# 5. How many movies were released in 2008?
total_2008 = df.where((df.release_year==2008) &  (df.type  == "Movie")).count()
print("Total movies released in 2008 is:",total_2008)

# total_2008.repartition(1).write.csv('Total_movie_in_2008', mode='overwrite')



# 6.List all the movies whose duration is greater than 100 mins ?
def duration_100(replace_min):
    return replace_min.replace('min','')

durationUDF = udf(duration_100)

filter_movie = df.filter(df['type']=='Movie').select(df['title'],df['duration'])

replacing_min = filter_movie.select(df['title'],durationUDF(filter_movie['duration']))
duration_100 =  replacing_min.where(replacing_min['duration_100(duration)']>100)

duration_100.show()

      #OUTPUT IN CSV
duration_100.repartition(1).write.csv('Duration>100',header=True, mode='overwrite')

      #OP Saved in Postgresql
duration_100.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                      dbtable='Duration_100', user='fusemachines', password='hello123').mode('overwrite').save()



# 7.List movies played by “Kareena Kapoor” ?
split_cast=df.select(df.cast,df.title).withColumn('cast',F.explode(F.split('cast',',')))
kareena_movies = split_cast.filter(split_cast['cast']=='Kareena Kapoor')
kareena_movies.show()

    #OUTPUT IN CSV
kareena_movies.repartition(1).write.csv('Kareena_movies',header=True, mode='overwrite')

      #OP Saved in Postgresql
kareena_movies.write.format('jdbc').options(url='jdbc:postgresql://localhost:5432/postgres', driver='org.postgresql.Driver',
                                   dbtable='Kareena_movies', user='fusemachines', password='hello123').mode('overwrite').save()


