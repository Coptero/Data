from pyspark import SparkConf
from pyspark import SparkContext
import sys
sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/")
from  job import  CopteroRODHelpdesk24hJob

from pyspark.sql import SparkSession
import argparse


class CopteroRODHelpdesk24hApp :

    def main():
        AppName = "Coptero ROD Helpdesk 24h Batch"
        # spark = SparkSession.builder.appName(AppName).getOrCreate()
        # Construct the argument parser
        ap = argparse.ArgumentParser()
        # Add the arguments to the parser
        ap.add_argument("-c", "--conf", required=True, help="s3 conf file path")
        ap.add_argument("-f", "--file", required=True, help="s3 ingest file path")
        args = vars(ap.parse_args())

        s3confPath = str(args['conf'])
        s3filePath = str(args['file'])

        if any(s3filePath):
            conf = SparkConf()
            conf.setAppName(AppName)
            sc = SparkContext(conf=conf)
            CopteroRODHelpdesk24hJob.runJob(sc, s3confPath, s3filePath)
        else:
            sys.exit(1)

    if __name__ == "__main__":
        main()
