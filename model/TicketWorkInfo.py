from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getWISchema(self, file):
    validationSchema = file.schema
    corrupt_file_schema = [StructField("_corrupt_record", StringType(), True)]
    StructType(corrupt_file_schema + validationSchema.fields)
    return validationSchema


def workInfoHPDColumns(file, spark):
    AdminNumberTags = ['ticket_id', 'modification_date_time', 'work_info_type_id', 'security', 'view_access',
                       'work_blocked', 'work_info_summary', 'work_info_notes', 'work_info_submitter', 'instanceid']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
