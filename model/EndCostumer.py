from pyspark.sql import *


class EndCustomer:

    def endCustomerColumns(file, spark):
        AdminNumberTags = ['operating_le', 'end_customer_correct']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
