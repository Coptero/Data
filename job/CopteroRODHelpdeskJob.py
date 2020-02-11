from pyspark.sql import SparkSession

from model import TicketDetailHelpdesk
from model.LogESIndex import LogESIndex
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.ElasticDsl import ElasticDsl
from Dsl.AlertsDsl import AlertDsl
from pyspark.sql import *
import logging

from model.TicketDetailHelpdesk import getIncidSchema
from utils.SparkJob import SparkJob
from Dsl.RemedyDsl import RemedyDsl


class CopteroRODHelpdeskJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        logStatus = LogESIndex.startLogStatus(s3filePath)
        dfCount = 0
        try:
            logging.info(
                "Start batch Coptero ROD for s3confPath:" + s3confPath + "--------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath,
                                                              S3FilesDsl.readFileSchema(s3filePath, getIncidSchema))

            logging.info("fileDetailHelpdesk.count().." + validatedRecords.count())
            rodTicketDetailHelpdesk = TicketDetailHelpdesk.detailHPDColumns(validatedRecords)
            logging.info("rodTicketDetailHelpdesk.count().." + rodTicketDetailHelpdesk.count())
            # val cisClosedDates = getCIsLastClosedDates(rodTicketDetailHelpdesk)
            esIndex = RemedyDsl.buildESIndex("helpdesk", rodTicketDetailHelpdesk, s3confPath, s3filePath)
            # TODO ? esIndex.as[IncidESIndex]with Option[String] = None
            logging.info("Persisting ES index..")
            dfCount = esIndex.count.toInt
            logging.info("indexDataFrame.count.." + dfCount)
            try:
                ElasticDsl.writeMappedESIndex(esIndex, "copt-rod-closed-{ticket_max_value_partition}", "ticket_id")
            except Exception as e:
                if e.getMessage.contains("index_closed_exception"):
                    raise e
                else:
                    logging.info("catched index_closed_exception: " + e.getMessage)

            AlertDsl.checkCount("copt-rod-closed-*", s3filePath, dfCount)

            # ?? logStatus = logStatus.copy(success=true, count=dfCount)
            logging.info("End batch Coptero ROD ----------------------------------------------------")
        except Exception as e:
            logStatus = logStatus.copy(success=False, exception=e.getMessage)
            logging.error("catched: " + e.getMessage)
            raise e
        finally:
            logDataFrame = spark.createDataFrame(logStatus.copy(end_date=Some(new
            SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance.getTime))))
            AlertDsl.writeESLogIndex(logDataFrame, "copt-rod-log-")
