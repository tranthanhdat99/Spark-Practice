
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def parse_numemp(no_employees):
    if no_employees.lower() == '500-1000' or no_employees.lower() == 'more than 1000':
        return 1
    else: return 0

parse_numemp_udf = udf(parse_numemp, returnType=IntegerType())
