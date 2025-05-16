from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark SQL Join Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path_1 = "/data/dataframe-join/d1/"
    flight_1 = spark.read.json(path_1)

    flight_1.printSchema()

    path_2 = "/data/dataframe-join/d2/"
    flight_2 = spark.read.json(path_2)

    flight_2.printSchema()


    flight_1.createOrReplaceTempView('flight_1_data')
    flight_2.createOrReplaceTempView('flight_2_data')

    query_1 = '''
        select f1.id, f1.dest, f1.dest_city_name, 
        to_date(f1.fl_date, 'M/d/yyyy') as flt_date,
        f1.origin, f1.origin_city_name,
        f2.cancelled
        from flight_1_data f1
        join flight_2_data f2
        on f1.id = f2.id
        where f2.cancelled == 1 and f1.dest_city_name == 'Atlanta, GA'
        and year(to_date(f1.fl_date, 'M/d/yyyy')) == 2000
        order by flt_date desc
    '''

    spark.sql(query_1).show()

    query_2 = '''
        select f1.dest,
        year(to_date(f1.fl_date, 'M/d/yyyy')) as fl_year,
        sum(f2.cancelled) as num_cancelled_flight
        from flight_1_data f1
        join flight_2_data f2
        on f1.id = f2.id
        group by f1.dest, year(to_date(f1.fl_date, 'M/d/yyyy'))
        order by num_cancelled_flight desc
    '''

    spark.sql(query_2).show()