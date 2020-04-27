from pyspark import SparkConf
from pyspark import SparkContext
import sys
sys.path.append("C:/Users/gonza/Downloads/LABSpark/")
import argparse
from LabCopteroPrediccionJob import LabCopteroPrediccionJob
class CopteroRODProblemsApp:

    def main():
        AppName = "Algoritmo prediccion "
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
            LabCopteroPrediccionJob.runJob(sc, s3confPath, s3filePath)
        else:
            sys.exit(1)

    if __name__ == "__main__":
        main()
