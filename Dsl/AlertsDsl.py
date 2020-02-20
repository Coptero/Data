# Faltan imports
import sys
folderPath = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folderPath)
import logging
from pyspark.sql import *
from datetime import datetime
from pyspark import *
from model.Alert import Alert
from elasticsearch import Elasticsearch
from Dsl.ElasticDsl import ElasticDsl


class AlertDsl():
    # Falta el implicit spark SparkSession
    def checkCount(indexName, fileName, dfCount, spark):
        logging.info('dfCount.. ' + dfCount)
        path = fileName.replace(':', r'\\:').replace("/", r"\\/")
        qResultDF = spark.sqlContext.read \
            .option("es.resource", "indexName") \
            .option("es.query", r"?q=file:\'' + path + '\'' ") \
            .format("org.elasticsearch.spark.sql") \
            .load()
        # ¿Equivalente de qResultDF = spark.esDF("${indexName}", "?q=file:\"" + path + "\"").select("ticket_id") ?
        qResultDF.cache
        queryCount = qResultDF.count
        qResultDF.unpersist
        logging.info("queryCount.. " + queryCount)
        if dfCount != queryCount:
            alertDataFrame = spark.createDataFrame([Alert(fileName, dfCount, queryCount, datetime.now().strftime("%Y%m%d%H%M%S"))])
            ElasticDsl.writeESAlertsIndex(alertDataFrame)
