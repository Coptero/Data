import sys

sys.path.append("C:/Users/gonza/Downloads/Data-master/")
import logging
from pyspark.sql import *
from datetime import datetime
from pyspark import *

from Dsl.ElasticDsl import ElasticDsl
from Dsl.S3FilesDsl import S3FilesDsl


class AlertDsl:
    def checkCount(indexName, fileName, dfCount, spark, conf):
        prefix = S3FilesDsl.readConfigJson(conf).elastic_env_index_prefix
        sqlContext = SQLContext(spark)
        logging.info('dfCount.. ' + str(dfCount))
        path = fileName.replace(':', '\\:').replace("/", "\\/")
        qResultDF1 = sqlContext.read \
            .option("es.resource",prefix + indexName) \
            .option("es.query", "?q=file:\''" + path + " '\'") \
            .format("org.elasticsearch.spark.sql") \
            .load()
        # ¿Equivalente de qResultDF = spark.esDF("${indexName}", "?q=file:\"" + path + "\"").select("ticket_id") ?
        qResultDF = qResultDF1.select("ticket_id")
        qResultDF.cache()
        queryCount = qResultDF.count()
        qResultDF.unpersist()
        logging.info("queryCount.. " + str(queryCount))
        if dfCount != queryCount:
            alertDataFrame = sqlContext.createDataFrame(
                [(fileName, dfCount, queryCount, datetime.now().strftime("%Y%m%d%H%M%S"))],
                ["file", "expected_count", "result_count", "date"])
            ElasticDsl.writeESAlertsIndex(alertDataFrame, conf)
