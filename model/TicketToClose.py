from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


class TicketToClose:
    def getClosedSchema(spark):
        validationSchema = StructType([
            StructField("ticket_id", StringType(), True),
            StructField("status_id", StringType(), True),
            StructField("_corrupt_record", StringType(), True)
        ])

        return validationSchema

    def detailClosedColumns(file, spark):
        AdminNumberTags = ['ticket_id', 'status_id']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
