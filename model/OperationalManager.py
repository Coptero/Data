from dataclasses import dataclass
from pyspark.sql import *


@dataclass
class OperationalManager(object):
    operating_company_name: str
    operating_le: str
    operational_manager: str

    def operationalManagerColumns(file):
        file.map(lambda r: OperationalManager(r.getString(0), r.getString(1), r.getString(2)))
