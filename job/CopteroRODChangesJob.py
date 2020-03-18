import sys
sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/")
from model import TicketDetailHelpdesk
from model.LogESIndex import LogESIndex, startLogStatus, logESindexSchema
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl
from pyspark.sql import *
import logging
from datetime import datetime
from model.TicketDetailChanges import getCRQSchema, detailCHGColumns
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import RemedyDsl
import copy


class CopteroRODChangesJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        logStatus = startLogStatus(s3filePath)
        dfCount = 0
        try:
            logging.info(
                "Start batch Coptero ROD for s3confPath:" + s3confPath + "--------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath,
                                                              S3FilesDsl.readFileSchema(s3filePath, getCRQSchema(s3filePath),spark), spark)
            #rodTicketDetailChanges = detailCHGColumns(validatedRecords)
            #            esIndexCHG = RemedyDsl.buildESIndex("changes", rodTicketDetailChanges, s3confPath, s3filePath, spark)

            esIndexCHG = RemedyDsl.buildESIndex("changes", validatedRecords, s3confPath, s3filePath, spark)

            logging.info("Persisting ES index..")
            #dfCount = esIndexCHG.count().toInt
            #logging.info("indexDataFrame.count.." + dfCount)
            try:
                ElasticDsl.writeMappedESIndex(esIndexCHG, "copt-rod-crq-{ticket_max_value_partition}", "ticket_id")
            except Exception as e:
                message = str(e)
                if message.find("index_closed_exception"):
                    raise e
                else:
                    # TODO saveToEs {partitioned} works fine but ends with exception ?¿
                    logging.info("catched index_closed_exception: " + str(e))

            #AlertDsl.checkCount("copt-rod-closed-*", s3filePath, dfCount,spark)

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
            logStatus.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            logStatus_data = logESindexSchema(logStatus.file, logStatus.count, logStatus.success, logStatus.exception,logStatus.start_date,logStatus.end_date)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus_data))
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-")
