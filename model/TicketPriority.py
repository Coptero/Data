from pyspark import *


def TicketPriority(file, spark):
    AdminNumberTags = ['priority_id', 'priority_desc']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
