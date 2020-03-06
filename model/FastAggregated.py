from pyspark import *

class FastAggregated:

    def aggColumns(file):
        AdminNumberTags = ['nni_group_id', 'nni_group_name', 'nni_item_name', 'pe_name', 'order_num', 'nni_carr',
                           'nni_resource', 'aggr_resource']
        for c, n in zip(file.columns, AdminNumberTags):
            file = file.withColumnRenamed(c, n)

        return file