from pyspark import *


class TicketImpact:

    def impactColumns(file):
        AdminNumberTags = ['impact_id', 'impact_desc']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
