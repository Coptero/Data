from pyspark.sql import *


class Customer:

    def customerColumns(file, spark):
        AdminNumberTags = ['operating_company_name', 'customer_correct']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
