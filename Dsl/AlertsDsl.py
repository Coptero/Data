import sys

sys.path.append("C:/Users/gonza/Downloads/Data-master/")
import logging
from pyspark.sql import *
from datetime import datetime
from pyspark import *

from Dsl.ElasticDsl import ElasticDsl


class AlertDsl:
    def checkCount(indexName, fileName, dfCount, spark, conf):
        logging.info('dfCount.. ' + dfCount)
        path = fileName.replace(':', r'\\:').replace("/", r"\\/")
        qResultDF = spark.sqlContext.read \
            .option("es.resource", "indexName") \
            .option("es.query", r"?q=file:\''" + path + " '\'") \
            .format("org.elasticsearch.spark.sql") \
            .load()
        # ¿Equivalente de qResultDF = spark.esDF("${indexName}", "?q=file:\"" + path + "\"").select("ticket_id") ?
        qResultDF.cache()
        queryCount = qResultDF.count()
        qResultDF.unpersist()
        logging.info("queryCount.. " + queryCount)
        if dfCount != queryCount:
            alertDataFrame = spark.createDataFrame(
                [(fileName, dfCount, queryCount, datetime.now().strftime("%Y%m%d%H%M%S"))],
                ["file", "expected_count", "result_count", "date"])
            ElasticDsl.writeESAlertsIndex(alertDataFrame, conf)

