from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark Source Sink Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path = '/data/source-and-sink/flight-time.parquet'

    flight = spark.read.parquet(path)

    flight.printSchema()

    #flight.show(5)

    flight.write\
    .mode('overwrite')\
    .option('maxRecordsPerFile', 10000)\
    .json('/data/source-and-sink/json')

    flight.write\
    .mode('overwrite')\
    .option('maxRecordsPerFile', 10000)\
    .format('avro')\
    .save('/data/source-and-sink/avro')

    flight_js = spark.read\
    .json('/data/source-and-sink/json')

    flight_js.createOrReplaceTempView('flight_js_data')

    query = '''
    select dest, dest_city_name,
    fl_date, origin,
    origin_city_name, cancelled
    from flight_js_data
    where cancelled = 1
    and dest = 'ATL' and extract(year from fl_date) = 2000
    order by fl_date desc
    '''

    spark.sql(query).show()

    flight_av = spark.read\
    .format('avro')\
    .load('/data/source-and-sink/avro')

    flight_av.createOrReplaceTempView('flight_av_data')

    query2 = '''
    select op_carrier, origin,
    sum(cancelled) as num_cancelled_flight
    from flight_av_data
    group by op_carrier, origin
    order by op_carrier, origin
    '''

    spark.sql(query2).show()

    spark.stop()





