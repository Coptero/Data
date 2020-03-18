from pyspark import SparkConf
from pyspark import SparkContext
import sys
sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/")
import argparse
from  pyspark.sql import SparkSession
from job.CopteroRODProblemsJob import CopteroRODProblemsJob
class CopteroRODProblemsApp:

    def main():
        AppName = "Coptero ROD Problems every 8 hours Batch"
        ap = argparse.ArgumentParser()
        ap.add_argument("-c", "--conf", required=True, help="s3 conf file path")
        ap.add_argument("-f", "--file", required=True, help="s3 ingest file path")
        args = vars(ap.parse_args())

        s3confPath = str(args['conf'])
        s3filePath = str(args['file'])
        if any(s3filePath):
            conf = SparkConf()
            conf.setAppName(AppName)
            sc = SparkContext(conf=conf)
            CopteroRODProblemsJob.runJob(sc, s3confPath, s3filePath)
        else:
            sys.exit(1)

    if __name__ == "__main__":
        main()
