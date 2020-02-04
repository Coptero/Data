from pyspark.sql import *


# TODO r.getAs("gw_carr_id") ????
def nnil2Columns(file, spark):
    FastNNIL2 = ['sc_name', 'sc_id', 'order_num', 'path_name', 'pe_id', 'dev_id', 'value', 'carr', 'resource',
                 'port_resource', 'ne_carr', 'nni_carr', 'nni_carr_id', 'gw_carr', 'gw_carr_id']
    for c, n in zip(file.columns, FastNNIL2):
        file = file.withColumnRenamed(c, n)

