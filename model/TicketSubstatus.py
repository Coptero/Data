from pyspark import *


def TicketSubstatus(file, spark):
    AdminNumberTags = ['substatus_id', 'substatus_desc', 'status_id']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
