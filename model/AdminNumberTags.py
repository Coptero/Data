from pyspark.sql import *


def antagsColumns(file, spark):
    AdminNumberTags = ['admin_number', 'tags']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
