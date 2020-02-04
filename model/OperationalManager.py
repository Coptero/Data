from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def operationalManagerColumns(file, spark):
    AdminNumberTags = ['operating_company_name', 'operating_le', 'operational_manager']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
