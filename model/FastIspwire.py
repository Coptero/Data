from pyspark.sql import *


class FastIspwire:
    def ispwireColumns(file, spark):
        AdminNumberTags = ['circuit_isp_wire', 'circuit_id']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file
