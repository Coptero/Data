from dataclasses import dataclass
from pyspark.sql import *


class FastServiceCircuit:
    # TODO r.getAs("gw_carr") ????
    def scColumns(file):
        scColumns = ['sc_name', 'sc_id', 'service_id', 'order_num', 'path_name', 'pe_id', 'dev_id', 'value', 'carr',
                    'resource', 'port_resource', 'ne_carr', 'nni_carr', 'nni_carr_id','gw_carr', 'gw_carr_id']
        for c, n in zip(file.columns, scColumns):
            file = file.withColumnRenamed(c, n)
        return file
    #      if(r.getDecimal(6)!=null){r.getDecimal(6).toString}else{"none"}, //dev_id
    #      if(r.getDecimal(14)!=null){r.getDecimal(14).toString}else{"none"})) //gw_carr_id)
