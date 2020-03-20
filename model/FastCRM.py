from pyspark.sql import *


class FastCRM:
    def crmColumns(file, spark):
        AdminNumberTags = ['circuit_crm', 'circuit_id']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
