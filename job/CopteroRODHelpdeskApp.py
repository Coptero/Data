from pyspark import SparkConf
from pyspark import SparkContext
from job.CopteroRODHelpdeskJob import CopteroRODHelpdeskJob
import sys


class CopteroRODHelpdeskApp:
    AppName = "Coptero ROD Helpdesk 5min Continuous Batch"

    parsedArgs = CopteroArgumentsParser.parse(args, CopteroArguments())
    if any(parsedArgs):
        conf = SparkConf()
        conf.setAppName(AppName)
        sc = SparkContext(conf=conf)
        CopteroRODHelpdeskJob.runJob(sc, parsedArgs.s3confPath, parsedArgs.s3filePath)
    else:
        sys.exit(1)
