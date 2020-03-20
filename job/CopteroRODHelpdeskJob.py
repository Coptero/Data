import sys
folderPath = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folderPath)
from pyspark.sql import *
from model import TicketDetailHelpdesk
from model.LogESIndex import LogESIndex, startLogStatus, logESindexSchema
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl
from model.AdminNumberTags import AdminNumberTags
import logging
from datetime import datetime
from model.TicketDetailHelpdesk import getIncidSchema
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import RemedyDsl, persistAgentSmc
import copy


class CopteroRODHelpdeskJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        conf = s3confPath
        logStatus = startLogStatus(s3filePath)
        dfCount = 0
        try:
            logging.info(
                "Start batch Coptero ROD for s3confPath:" + s3confPath + "--------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath,
                                                              S3FilesDsl.readFileSchema(s3filePath,
                                                                                        getIncidSchema(s3filePath),
                                                                                        spark), spark, conf)

            logging.info("fileDetailHelpdesk.count().." + str(validatedRecords.count()))
            rodTicketDetailHelpdesk = TicketDetailHelpdesk.detailHPDColumns(validatedRecords)
            logging.info("rodTicketDetailHelpdesk.count.." + str(rodTicketDetailHelpdesk.count()))
            # val cisClosedDates = getCIsLastClosedDates(rodTicketDetailHelpdesk)
            esIndex = RemedyDsl.buildESIndex("helpdesk", rodTicketDetailHelpdesk, s3confPath, s3filePath, spark)
            # TODO ? esIndex.as[IncidESIndex]with Option[String] = None
            logging.info("Persisting ES index..")
            dfCount = esIndex.count()
            # logging.info("indexDataFrame.count.." + dfCount)
            try:
                ElasticDsl.writeMappedESIndex(esIndex, "copt-rod-closed-{ticket_max_value_partition}", "ticket_id", conf)
            except Exception as e:
                message = str(e)
                if message.find("index_closed_exception"):
                    raise e
                else:
                    # TODO saveToEs {partitioned} works fine but ends with exception ?¿
                    logging.info("catched index_closed_exception: " + str(e))

            persistAgentSmc(esIndex, s3confPath, spark)

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
            logStatus.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            logStatus_data = logESindexSchema(logStatus.file, logStatus.count, logStatus.success, logStatus.exception,
                                              logStatus.start_date, logStatus.end_date)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus_data))
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-", conf)
