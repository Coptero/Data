import logging
from pyspark.sql import SparkSession


class SparkJobBase(logging):
    C = None

    def runJob(sc, s3confPath, s3filePath):
        pass


class SparkJob(SparkJobBase):
    C = SparkSession
