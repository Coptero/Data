import sys

sys.path.append("C:/Users/gonza/Downloads/LABSpark/")
from model import TicketDetailHelpdesk
from model.LogESIndex import LogESIndex
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl
from pyspark.sql import *
# from pyspark.sql import SparkSession
# from pyspark import SparkContext
import logging
from datetime import datetime
from model.TicketWorkInfo import getWISchema, workInfoHPDColumns
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import RemedyDsl
import copy
import pyspark.sql.functions as F
from utils.Utils import Utils


class CopteroRODWorkinfoJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        logStatus = LogESIndex.startLogStatus(s3filePath)
        dfCount = 0
        try:
            logging.info(
                "Start batch Coptero ROD for s3confPath: " + s3confPath + "--------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath,
                                                              S3FilesDsl.readFileSchema(s3filePath,
                                                                                        getWISchema(s3filePath), spark,
                                                                                        s3confPath),
                                                              spark,
                                                              s3confPath)
            # Necesitamos ? rodTicketWorkinfoHelpdesk = workInfoHPDColumns(validatedRecords)

            indexAgentSmcCluster = RemedyDsl.getAgentSmcCluster(validatedRecords, s3confPath)

            indexWithRelations = RemedyDsl.getRelations(indexAgentSmcCluster, s3confPath)

            partitioned = indexWithRelations \
                .withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id")) \
                .withColumn("file", F.lit("s3filePath")) \
                .withColumn("work_info_category", Utils.getWorkInfoCategory("work_info_notes"))

            dfCount = partitioned.count()
            logging.info("Persisting ES index..")

            logging.info("indexWorkInfoDataFrame.count().." + dfCount)
            try:
                ElasticDsl.writeMappedESIndex(partitioned, "copt-rod-wif-{ticket_max_value_partition}", "instanceid",
                                              s3confPath)
            except Exception as e:
                if e.getMessage.contains("index_closed_exception"):
                    raise e
                else:
                    # TODO saveToEs {partitioned} works fine but ends with exception ?¿
                    logging.info("catched index_closed_exception: " + e.getMessage)

            # AlertDsl.checkCount("copt-rod-closed-*", s3filePath, dfCount,spark)

            logStatus = copy.deepcopy(logStatus)
            logStatus.success = True
            # logStatus.count = dfCount
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
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-", s3confPath)
