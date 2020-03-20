from pyspark.sql import *


class FastSupplier:
    def supplierColumns(file, spark):
        AdminNumberTags = ['circuit_supplier_minerva', 'circuit_id']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
