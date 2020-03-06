from pyspark import *


class TicketUrgency:

    def urgencyColumns(file):
        AdminNumberTags = ['urgency_id', 'urgency_desc']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
