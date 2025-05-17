from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark Window Function Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path = "/data/window-function/summary.parquet"
    summary = spark.read\
    .parquet(path)

    summary.printSchema()

    summary.createOrReplaceTempView('summary_data')

    query = '''
        select country,
        weeknumber, numinvoices,
        totalquantity, invoicevalue,
        rank() over (partition by country order by country asc, invoicevalue desc) as rank
        from summary_data
    '''

    spark.sql(query).show()

    query_2 = '''
    select country,
        weeknumber, numinvoices,
        totalquantity, invoicevalue,
        coalesce(
            round(
            (invoicevalue - lag(invoicevalue) over (partition by country order by country, weeknumber asc))*100
            /lag(invoicevalue) over (partition by country order by country, weeknumber asc),2
            )
        , 0.0) as percentgrowth,
        round(
            sum(invoicevalue) over 
            (partition by country 
            order by country, weeknumber 
            rows between unbounded preceding and current row)
            ,2) as accummulateValue
        from summary_data
    '''

    spark.sql(query_2).show()



