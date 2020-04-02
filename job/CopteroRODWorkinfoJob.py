import sys

sys.path.append("C:/Users/gonza/Downloads/LABSpark/")
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl
from pyspark.sql import *

import logging
from datetime import datetime
from model.TicketWorkInfo import getWISchema, workInfoHPDColumns
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import getAgentSmcCluster, getRelations
import copy
import pyspark.sql.functions as F
from utils.Utils import Utils
from model.LogESIndex import LogESIndex, startLogStatus, logESindexSchema


class CopteroRODWorkinfoJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        logStatus = startLogStatus(s3filePath)
        dfCount = 0
        try:
            logging.info(
                "Start batch Coptero ROD for s3confPath: " + s3confPath + "--------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath,
                                                              S3FilesDsl.readFileSchema(s3filePath, getWISchema(s3filePath), spark),
                                                              spark,
                                                              s3confPath)

            indexAgentSmcCluster = getAgentSmcCluster(validatedRecords, s3confPath, spark)

            indexWithRelations = getRelations(indexAgentSmcCluster, s3confPath, spark)

            partitioned = indexWithRelations \
                .withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id")) \
                .withColumn("file", F.lit(s3filePath)) \
                .withColumn("work_info_category", Utils.getWorkInfoCategory("work_info_notes"))

            dfCount = partitioned.count()
            logging.info("Persisting ES index..")

            logging.info("indexWorkInfoDataFrame.count().." + str(dfCount))
            try:
                ElasticDsl.writeMappedESIndex(partitioned, "copt-rod-wif-{ticket_max_value_partition}", "instanceid",
                                              s3confPath)
            except Exception as e:
                message = str(e)
                if message.find("index_closed_exception"):
                    raise e
                else:
                    # TODO saveToEs {partitioned} works fine but ends with exception ?¿
                    logging.info("catched index_closed_exception: " + str(e))

            AlertDsl.checkCount("copt-rod-wif*", s3filePath, dfCount, spark, s3confPath)

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
            logStatus_data = logESindexSchema(logStatus.file, logStatus.count, logStatus.success, logStatus.exception,
                                              logStatus.start_date, logStatus.end_date)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus_data))
