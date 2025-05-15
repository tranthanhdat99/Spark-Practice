from pyspark.sql import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, when)
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Spark SQL Exercise") \
        .master("spark://spark:7077") \
        .config("spark.sql.catalogImplementation","hive")\
        .getOrCreate()
    
    path = "/data/spark-sql/survey.csv"
    survey = spark.read\
    .csv(path,header=True)

    survey_filter = survey\
    .where\
        (
            (F.lower(col('gender')).isin('male', 'm')) &
            (col('age') < 40)
        )\
    
    result = survey_filter\
          .groupBy('country')\
          .agg(F.count('gender').alias('total'))    

    result.select('country', 'total')\
    .orderBy(col('total').desc(), col('country'))\
    .show()


    trans_survey = survey\
    .withColumn('num_male',
                when(F.lower(col('gender')).isin('male','man','m'), 1)
                .otherwise(0)
                )\
    .withColumn('num_female',
                when(F.lower(col('gender')).isin('female','woman','w'), 1)
                .otherwise(0)
                )
    
    result_2 = trans_survey\
    .groupBy('country')\
    .agg(
        F.sum('num_male').alias('num_male'),
        F.sum('num_female').alias('num_female')
    )\
    .select('country', 'num_male', 'num_female')\
    .orderBy('country')\
    .show()

