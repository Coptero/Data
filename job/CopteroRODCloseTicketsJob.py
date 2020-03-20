import sys

sys.path.append("C:/Users/gonza/Downloads/LABSpark/")
from pyspark.sql import *
from model import TicketDetailHelpdesk
from model.LogESIndex import LogESIndex, startLogStatus, logESindexSchema
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl

# from pyspark.sql import SparkSession
# from pyspark import SparkContext
import logging
from datetime import datetime
from model.TicketToClose import getClosedSchema
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import RemedyDsl
import copy


class CopteroRODCloseTicketsJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        logStatus = startLogStatus(s3filePath)
        dfCount = 0
        try:
            logging.info(
                "Start batch Coptero ROD for s3confPath:" + s3confPath + "--------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath,
                                                              S3FilesDsl.readFileSchema(s3filePath,
                                                                                        getClosedSchema(s3filePath),
                                                                                        spark,
                                                                                        s3confPath), spark)

            logging.info("validatedRecords.count().." + str(validatedRecords.count()))
            ticketToCloseDS = TicketToClose.detailClosedColumns(validatedRecords)
            logging.info("ticketToCloseDS.count().." + str(ticketToCloseDS.count()))

            esIndex = ticketToCloseDS\
                .withColumn("open", F.lit(Constants.OPEN_NO))\
                .withColumn("file", F.lit(s3filePath))\
                .withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id"))

            logging.info("Persisting ES index..")
            dfCount = esIndex.count()
            # logging.info("indexDataFrame.count.." + dfCount)
            try:
                ElasticDsl.writeMappedESIndex(esIndex, "copt-rod-closed-{ticket_max_value_partition}", "ticket_id", s3confPath)
            except Exception as e:
                message = str(e)
                if message.find("index_closed_exception"):
                    raise e
                else:
                    # TODO saveToEs {partitioned} works fine but ends with exception ?¿
                    logging.info("catched index_closed_exception: " + str(e))

            # AlertDsl.checkCount("copt-rod-closed-*", s3filePath, dfCount,spark)

            logStatus = copy.deepcopy(logStatus)
            logStatus.success = True
            logStatus.count = dfCount
            logStatus.exception = ""
            logStatus.end_date = ""
            logging.info("End batch Coptero ROD ----------------------------------------------------")
        except Exception as e:
            logStatus = copy.deepcopy(logStatus)
            logStatus.success = False
            logStatus.count = dfCount
            logStatus.exception = str(e)
            logStatus.end_date = ""
            logging.error("catched: " + str(e))
            raise e
        finally:
            sqlContext = SQLContext(spark)
            # logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus))
            # statusSchema = logStatusSchema()
            # logDataFrame = sqlContext.createDataFrame([logStatus.file, logStatus.count, logStatus.success, logStatus.exception,logStatus.start_date,logStatus.end_date],statusSchema)
            logStatus.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            logStatus_data = logESindexSchema(logStatus.file, logStatus.count, logStatus.success, logStatus.exception,
                                              logStatus.start_date, logStatus.end_date)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus_data))
            # logDataFrame.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            logDataFrame.show(5)
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-", s3confPath)