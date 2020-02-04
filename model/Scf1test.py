from dataclasses import dataclass
from pyspark.sql import *


@dataclass
class Scf1test(object):
    admin_number: str
    access_supplier: str
    routing_role: str
    tbs_pe_access_mode: str
    order_number: str
    carrier: str
    intf: str

    def scf1Columns(file):
        file.map(lambda r: Scf1test(
            r.getString(0),
            r.getString(1),
            r.getString(2),
            r.getString(3),
            r.getString(4),
            r.getString(5),
            r.getString(6))
                 )
