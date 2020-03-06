from pyspark.sql import *

class AdminNumberTags:

    def antagsColumns(file):
        AdminNumberTags = ['admin_number', 'tags']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)
        return file