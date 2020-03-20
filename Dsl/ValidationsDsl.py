import logging
import sys
sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/") 
from Dsl.ElasticDsl import ElasticDsl
from pyspark.sql import *
import pyspark.sql.functions as F


class ValidationsDsl():
    def validateTickets(s3filePath, tickets, spark, s3confPath):
        sqlContext = SQLContext(spark)
        corruptRecords = tickets.filter(tickets._corrupt_record.isNotNull() | tickets.ticket_id.isNull())
        corruptRecords.cache()
        corruptRecordsCount = corruptRecords.count()
        logging.info("corruptRecords.count.." + str(corruptRecordsCount))
        corruptRecords.unpersist()

        if corruptRecordsCount > 0:
            withS3path = corruptRecords.withColumn("file", F.lit(s3filePath))
            ElasticDsl.writeESCorruptRecordsIndex(withS3path, "copt-rod-corrupt-records-", s3confPath)

        validatedRecords = tickets.filter(tickets._corrupt_record.isNull() & tickets.ticket_id.isNotNull())
        validatedRecords.cache()
        logging.info("validatedRecords.count.." + str(validatedRecords.count()))
        validatedRecords.unpersist()

        return validatedRecords

