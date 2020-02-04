from pyspark import *


def TicketUrgency(file, spark):
    AdminNumberTags = ['urgency_id', 'urgency_desc']
    for c, n in zip(file.columns, AdminNumberTags):
        file = file.withColumnRenamed(c, n)
