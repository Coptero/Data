from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import *
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, BooleanType
from collections import namedtuple


# no estoy seguro de que esto sea así
@dataclass
class LogESIndex:
    pass
    ''''def __init__(self):
        file: str = None
        count: int = None
        success: bool = None
        exception: str = None
        start_date: str = None
        end_date: str = None
    '''

def startLogStatus(file):
    c = LogESIndex()
    c.file = file
    c.start_date = datetime.now().strftime("%Y%m%d%H%M%S")
    #file = file
    #start_date = datetime.now().strftime("%Y%m%d%H%M%S")
    return c

'''def logStatusSchema():
    logStatusSchema = StructType([
        StructField("file", StringType(), True),
        StructField("count", IntegerType(), True),
        StructField("success", BooleanType(), True),
        StructField("exception", StringType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True)
        ])
    return logStatusSchema
    '''

def logESindexSchema(file, count, success, exception, start_date, end_date):
    log_row = namedtuple('log_row', 'file count success exception start_date end_date'.split())
    log_date = [
        log_row(file, count, success, exception, start_date, end_date)
    ]

    return log_date


