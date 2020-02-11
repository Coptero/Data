from pyspark import *

class TicketSubstatus:

    def substatusColumns(file, spark):
        AdminNumberTags = ['substatus_id', 'substatus_desc', 'status_id']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file