from pyspark import *


class TicketStatus:

    def statusColumns(file):
        AdminNumberTags = ['status_id', 'status_desc']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
