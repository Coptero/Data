from pyspark.sql import *
import tbs.bigdata.dsl.S3FilesDsl.readFile
import model.FastAggregated
import model.FastByAdmin
import model.FastNNIL2
import model.FastPrincipal
import model.FastServiceCircuit


def postgreSQLAdminNumber(url, query, user, password, spark):
    jdbcDF = postgreSQL(url, query, user, password, spark)
    return fastColumns(jdbcDF)


def postgreSQL(url, query, user, password, spark):
    return spark.read\
        .format("jdbc")\
        .option("url", url)\
        .option("user", user)\
        .option("password", password)\
        .option("query", query)\
        .load()

def postgreSQLMain(url, query, user,password, spark):
    jdbcDF = readFile("s3://rpajares/scalaSparkJobs/exportQueryFASTprincipal.csv")
    return principalColumns(jdbcDF)


def postgreSQLServiceCircuit(url, query, user,password, spark):
    jdbcDF = postgreSQL(url, query, user, password, spark)
    return scColumns(jdbcDF)

def postgreSQLAggregated(url, query, user, password, spark):
    jdbcDF = postgreSQL(url, query, user, password, spark)
    return nnil2Columns(jdbcDF)