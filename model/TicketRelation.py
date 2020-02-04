from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getRelationSchema(spark):
    validationSchema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("ticket_type", StringType(), True),
        StructField("related_ticket_id", StringType(), True),
        StructField("related_ticket_type", StringType(), True),
        StructField("association_type", StringType(), True),
        StructField("submit_date", StringType(), True),
        StructField("relation_summary", StringType(), True),
        StructField("status", StringType(), True),
        StructField("submitter", StringType(), True),
        StructField("instanceid", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])

    return validationSchema


def relationColumns(file, spark):
    AdminNumberTags = ['relation_id', 'ticket_id', 'ticket_type', 'related_ticket_id', 'related_ticket_type',
                       'association_type',
                       'submit_date', 'relation_summary', 'status', 'submitter', 'instanceid']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
