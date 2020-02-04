from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def getRelationSchema(spark):
    ticket_id = [StructField("ticket_id", StringType(), True)]
    ticket_type = [StructField("ticket_type", StringType(), True)]
    related_ticket_id = [StructField("related_ticket_id", StringType(), True)]
    related_ticket_type = [StructField("related_ticket_type", StringType(), True)]
    association_type = [StructField("association_type", StringType(), True)]
    submit_date = [StructField("submit_date", StringType(), True)]
    relation_summary = [StructField("relation_summary", StringType(), True)]
    status = [StructField("status", StringType(), True)]
    submitter = [StructField("submitter", StringType(), True)]
    instanceid = [StructField("instanceid", StringType(), True)]
    corrupt_file_schema = [StructField("_corrupt_record", StringType(), True)]
    validationSchema = StructType(ticket_id + ticket_type + related_ticket_id + related_ticket_type +
                                  association_type + submit_date + relation_summary + status +
                                  submitter + instanceid + corrupt_file_schema)
    return validationSchema


def relationColumns(file, spark):
    AdminNumberTags = ['relation_id', 'ticket_id', 'ticket_type', 'related_ticket_id', 'related_ticket_type',
                       'association_type',
                       'submit_date', 'relation_summary', 'status', 'submitter', 'instanceid']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
