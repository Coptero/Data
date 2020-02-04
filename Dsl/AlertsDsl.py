# Faltan imports
import logging
from pyspark import *


class AlertDsl(logging):
    # Falta el implicit spark SparkSession
    def checkCount(indexName, fileName, dfCount, spark):
        logging.info('dfCount.. ' + dfCount)
        path = fileName.replace(":", "\\:").replace("/", "\\/")
        qResultDF = spark.esDF("${indexName}", "?q=file:\"" + path + "\"").select("ticket_id")
        qResultDF.cache
        queryCount = qResultDF.count
        qResultDF.unpersist

        logging.info("queryCount.. " + queryCount)

        if dfCount != queryCount:
            alertDataFrame =  spark.Seq(Alert(fileName, dfCount, queryCount, new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance.getTime))).toDF()
            writeESAlertsIndex(alertDataFrame)
