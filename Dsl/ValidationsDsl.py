import logging
from Dsl.ElasticDsl import ElasticDsl
import pyspark.sql.functions as F


class ValidationsDsl(logging):
    def validateTickets(s3filePath, tickets, spark):
        corruptRecords = spark.createDataFrame(tickets.filter(
            tickets._corrupt_record.isNotNull() | tickets.ticket_id.isNull()))  # TODO ticket_id nullable = true
        corruptRecords.cache()
        corruptRecordsCount = corruptRecords.count
        logging.info("corruptRecords.count.." + corruptRecordsCount)
        corruptRecords.unpersist()

        if corruptRecordsCount > 0:
            withS3path = corruptRecords.withColumn("file", F.lit(s3filePath)) #Lit not found, should be in pyspark.sql.functions
            ElasticDsl.writeESCorruptRecordsIndex(withS3path, "copt-rod-corrupt-records-")

        validatedRecords = spark.createDataFrame(
            tickets.filter(tickets._corrupt_record.isNull() & tickets.ticket_id.isNotNull()))
        validatedRecords.cache()
        logging.info("validatedRecords.count.." + validatedRecords.count)
        validatedRecords.unpersist()

        return validatedRecords
