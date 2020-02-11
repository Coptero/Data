from pyspark import *


def reportedSourceColumns(file, spark):
    AdminNumberTags = ['reported_source_id', 'reported_source_desc']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
    return file