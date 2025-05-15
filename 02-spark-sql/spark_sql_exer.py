from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark SQL Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path = "/data/spark-sql/survey.csv"
    survey = spark.read\
    .csv(path,header=True)

    survey.createOrReplaceTempView('survey_data')

    query = '''
        select country, count(1) as total
        from survey_data
        where lower(gender) in ('male', 'm') and age < 40
        group by country
        order by count(1) desc, country
    '''
    
    spark.sql(query).show()

    query_2 = '''
        select country,
        sum(case when lower(gender) in ('male','m','man') then 1 else 0 end) as num_male,
        sum(case when lower(gender) in ('female','w','woman') then 1 else 0 end) as num_female
        from survey_data
        group by country
        order by country;
    '''

    spark.sql(query_2).show()

    
