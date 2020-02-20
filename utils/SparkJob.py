import logging
import sys

folderPath = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folderPath)
from pyspark.sql import SparkSession


class SparkJobBase():
    C = None

    def runJob(sc, s3confPath, s3filePath):
        pass


class SparkJob(SparkJobBase):
    C = SparkSession
