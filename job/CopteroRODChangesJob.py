import sys
sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/")
from model import TicketDetailHelpdesk
from model.LogESIndex import LogESIndex
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl
from pyspark.sql import *
#from pyspark.sql import SparkSession
#from pyspark import SparkContext
import logging
from datetime import datetime
from model.TicketDetailChanges import getCRQSchema, detailCHGColumns
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import RemedyDsl
import copy


class CopteroRODChangesJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        logStatus = LogESIndex.startLogStatus(s3filePath)
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
                if e.getMessage.contains("index_closed_exception"):
                    raise e
                else:
                    # TODO saveToEs {partitioned} works fine but ends with exception ?¿
                    logging.info("catched index_closed_exception: " + e.getMessage)

            #AlertDsl.checkCount("copt-rod-closed-*", s3filePath, dfCount,spark)

            logStatus = copy.deepcopy(logStatus)
            logStatus.success = True
            #logStatus.count = dfCount
            logging.info("End batch Coptero ROD ----------------------------------------------------")
        except Exception as e:
            logStatus = copy.deepcopy(logStatus)
            logStatus.success = False
            logStatus.exception = e.getMessage
            logging.error("catched: " + e.getMessage)
            raise e
        finally:
            sqlContext = SQLContext(spark)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus))
            logDataFrame.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-")
