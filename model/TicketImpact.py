from pyspark import *


def impactColumns(file, spark):
    AdminNumberTags = ['impact_id', 'impact_desc']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
