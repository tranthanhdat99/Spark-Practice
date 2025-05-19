from pyspark.sql import *
from pyspark.sql.types import IntegerType

import numemp_util.numemp_util as numemp_util

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark UDF Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path = "/data/udf/survey.csv"
    survey = spark.read\
    .csv(path, header=True)

    survey.printSchema()

    spark.udf.register('numemp_collect', numemp_util.parse_numemp, IntegerType())

    survey.createOrReplaceTempView('survey_data')

    query = '''
    select age, gender, country,
    state, no_employees
    from survey_data
    where numemp_collect(no_employees) == 1
    '''
    
    spark.sql(query).show()

    spark.stop()
    