import sys
folderPath = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folderPath)
import pyspark.sql.functions as F
import logging
from utils.Utils import Utils
from utils.SparkJob import SparkJob
from model.LogESIndex import LogESIndex, startLogStatus, logESindexSchema
from model.TicketRelation import relationColumns, getRelationSchema
from Dsl.AlertsDsl import AlertDsl
from Dsl.ValidationsDsl import ValidationsDsl
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.ElasticDsl import ElasticDsl
from pyspark.sql import *
from datetime import datetime
import copy


class CopteroRODRelationJob(SparkJob):

    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        conf = s3confPath
        logStatus = startLogStatus(s3filePath)
        dfCountRelation = 0
        dfCountIncid = 0

        try:
            logging.info("Start batch Coptero ROD for s3confPath: " +
                        s3confPath + " -------------------------------------")
            validatedRecords = ValidationsDsl.validateTickets(s3filePath, S3FilesDsl.readFileSchema(
                s3filePath, getRelationSchema(s3filePath), spark), spark, conf)

            rodTicketRelation1 = relationColumns(validatedRecords, spark)
            rodTicketRelation = rodTicketRelation1.withColumn("relation_id", F.concat(rodTicketRelation1["ticket_id"],F.lit('-'), rodTicketRelation1["related_ticket_id"]))

            esIndexRel = rodTicketRelation.select('relation_id','ticket_id', 'ticket_type', 'related_ticket_id', 'related_ticket_type', 'association_type', 'submit_date', 'relation_summary', 'status', 'submitter', 'instanceid')
            partitioned = esIndexRel \
                .withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id")) \
                .withColumn("file", F.lit(s3filePath))

            dfCountRelation = partitioned.count()

            logging.info("Persisting ES index..")
            logging.info("indexRelationDataFrame.count().."+str(dfCountRelation))

            try:
                ElasticDsl.writeMappedESIndex(
                    partitioned, "copt-rod-rel-{ticket_max_value_partition}", "relation_id", conf)
            except Exception as ex:
                e = str(ex)
                if e.find("index_closed_exception"):
                    logging.info("catched index_closed_exception: " + e)
                else:
                    raise ex

            #AlertDsl.checkCount("copt-rod-rel-*", s3filePath, dfCountRelation)

            logStatus = copy.deepcopy(logStatus)
            logStatus.success = True
            logStatus.count = dfCountRelation
            logStatus.exception = ""
            logStatus.end_date = ""

            relationsDF = esIndexRel \
                .filter(esIndexRel.ticket_type == "Incident") \
                .groupBy("ticket_id") \
                .agg(F.collect_list("related_ticket_id").alias("relations")) \
                .withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id")) \
                .withColumn("file", F.lit(s3filePath))

            # TODO writeMappedESIndex CRQ Y PBI

            dfCountIncid = relationsDF.count()

            logging.info("Persisting ES index..")
            logging.info("relationsDF.count().."+ str(dfCountIncid))

            try:
                ElasticDsl.writeMappedESIndex(
                    relationsDF, "copt-rod-closed-{ticket_max_value_partition}", "ticket_id", conf)
            except Exception as ex:
                e = str(ex)
                if e.find("index_closed_exception"):
                    logging.info("catched index_closed_exception: " + e)
                else:
                    raise ex

            #AlertDsl.checkCount("copt-rod-closed-*", s3filePath, dfCountIncid)

            logStatus = copy.deepcopy(logStatus)
            logStatus.success = True
            logStatus.count = dfCountIncid
            logStatus.exception = ""
            logStatus.end_date = ""
            logging.info("End batch Coptero ROD ----------------------------------------------------")
        except Exception as ex:
            e = str(ex)
            logStatus.success = False
            logStatus.count = 0
            logStatus.exception = str(e)
            logStatus.end_date = ""
            logging.info("catched: " + e)
            raise ex
        finally:
            sqlContext = SQLContext(spark)
            logStatus.end_date = datetime.now().strftime("%Y%m%d%H%M%S")
            logStatus_data = logESindexSchema(logStatus.file, logStatus.count, logStatus.success, logStatus.exception, logStatus.start_date, logStatus.end_date)
            logDataFrame = sqlContext.createDataFrame(copy.deepcopy(logStatus_data))
            ElasticDsl.writeESLogIndex(logDataFrame, "copt-rod-log-", conf)
