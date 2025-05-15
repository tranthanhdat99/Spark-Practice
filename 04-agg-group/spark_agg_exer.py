from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark SQL Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path = "/data/agg-group/invoices.csv"
    invoices = spark.read\
    .csv(path,header=True)

    invoices.createOrReplaceTempView('invoice_data')

    query = '''
        select
        Country,
        year(to_timestamp(InvoiceDate, "dd-MM-yyyy H.mm")) as Year,
        count(InvoiceNo) as num_invoices,
        sum(Quantity) as total_quantity,
        sum(Quantity * UnitPrice) as invoice_value
        from invoice_data
        group by Country, year(to_timestamp(InvoiceDate, "dd-MM-yyyy H.mm"))
        order by Country, Year
    '''

    spark.sql(query).show()

    query_2 = '''
        select
        CustomerID,
        sum(Quantity * UnitPrice) as invoice_value
        from invoice_data
        where year(to_timestamp(InvoiceDate, "dd-MM-yyyy H.mm")) == 2010 and CustomerID is not null
        group by CustomerID
        order by sum(Quantity * UnitPrice) desc, CustomerID
        limit 10
    '''

    spark.sql(query_2).show()