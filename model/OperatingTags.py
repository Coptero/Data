from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class OperatingTags:

    def operatingTagsColumns(file):
        AdminNumberTags = ['operating_company_name', 'operating_le', 'operating_tags']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file