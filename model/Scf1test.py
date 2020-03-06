from dataclasses import dataclass
from pyspark.sql import *  # DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType


def scf1Columns(file):
    AdminNumberTags = ['admin_number', 'access_supplier', 'routing_role', 'tbs_pe_access_mode',
                       'order_number', 'carrier', 'intf']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
    return file