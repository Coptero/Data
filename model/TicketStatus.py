from pyspark import *


def TicketStatus(file, spark):
    AdminNumberTags = ['status_id', 'status_desc']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
