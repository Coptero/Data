import sys
folderPath = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folderPath)
import pyspark.sql.functions as F
import logging
import copy
from pyspark.sql import *
from datetime import datetime

from utils.Utils import Utils
from utils.SparkJob import SparkJob
from model.LogESIndex import LogESIndex, startLogStatus, logESindexSchema
from Dsl.AlertsDsl import AlertDsl
from model.TicketDetailProblems import getPBISchema, detailPBMColumns
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.RemedyDsl import RemedyDsl

class CopteroRODProblemsJob(SparkJob):

    def runJob(sparkSession, s3confPath, s3filePath):

        spark= sparkSession

        logStatus = startLogStatus(s3filePath)
        dfCount = 0

        try:
            logging.info("Start batch Coptero ROD for s3confPath: " +s3confPath +"-------------------------------------")

            validatedRecords  = ValidationsDsl.validateTickets(s3filePath, S3FilesDsl.readFileSchema(s3filePath, getPBISchema(s3filePath), spark), spark)

            rodTicketDetailProblems = detailPBMColumns(validatedRecords,spark)
            esIndexPBM = RemedyDsl.buildESIndex("problems", rodTicketDetailProblems, s3confPath, s3filePath,spark)

            dfCount = esIndexPBM.count()

            logging.info("Persisting ES indexes..")
            logging.info("indexProblemDataFrame.count().."+str(dfCount))

            try:
                ElasticDsl.writeMappedESIndex(esIndexPBM, "copt-rod-pbi-{ticket_max_value_partition}", "ticket_id")
            except Exception as ex:
                e = str(ex)
                if e.find("index_closed_exception"):
                    logging.info("catched index_closed_exception: " + e)
                else:
                    raise ex

            #AlertDsl.checkCount("copt-rod-pbi-*", s3filePath, dfCount)

            logStatus = copy.deepcopy(logStatus)
            logStatus.success = True
            logStatus.count = dfCount
            logStatus.exception = ""
            logStatus.end_date = ""

            logging.info("End batch Coptero ROD ----------------------------------------------------")
        except Exception as ex:
            e = str(ex)
            logStatus.success = False
            logStatus.count = dfCount
            logStatus.exception = e
            logStatus.end_date = ""
            logging.info("catched: " + e)
            raise ex

        finally:
            sqlContext = SQLContext(spark)
            logStatus.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            logStatus_data = logESindexSchema(logStatus.file, logStatus.count, logStatus.success, logStatus.exception, logStatus.start_date, logStatus.end_date)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus_data))
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-")
