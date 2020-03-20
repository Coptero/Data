from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getWISchema(spark):
    validationSchema = validationSchema = StructType([
        StructField("ticket_id", StringType(), True),
        StructField("submit_date", StringType(), True),
        StructField("work_info_type_id", StringType(), True),
        StructField("security", StringType(), True),
        StructField("view_access", StringType(), True),
        StructField("work_blocked", StringType(), True),
        StructField("work_info_summary", StringType(), True),
        StructField("work_info_notes", StringType(), True),
        StructField("work_info_submitter", StringType(), True),
        StructField("instanceid", StringType(), True),
        StructField("_corrupt_record", StringType(), True)
    ])
    return validationSchema


def workInfoHPDColumns(file, spark):
    AdminNumberTags = ['ticket_id', 'submit_date', 'work_info_type_id', 'security', 'view_access',
                       'work_blocked', 'work_info_summary', 'work_info_notes', 'work_info_submitter', 'instanceid']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
    return file
