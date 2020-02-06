from pyspark.sql import *
from datetime import datetime
import boto3
import json
import config.ConfigJson


class S3FilesDsl:
    def readConfigJson(s3confPath):
        s3Client = boto3.standard.withRegion(Regions.EU_CENTRAL_1).build  # EU (Frankfurt)
        bucketName = s3confPath.replace("s3://", "", 1).split("/", 0)
        path = s3confPath.replace("s3://" + bucketName + "/", "", 1)
        s3InputStream = s3Client.getObject(bucketName, path).getObjectContent
        jsonString = IOUtils.toString(s3InputStream)
        jsv = json.loads(jsonString)
        # ? fields = jsv.asJsObject.fields


        config.ConfigJson(
        fields("operational_path").convertTo[String],
        fields("tags_admin_path").convertTo[String],
        fields("tags_operating_path").convertTo[String],
        fields("fast_min_results").convertTo[Long],
        fields("fast_postgresql").convertTo[String],
        fields("fast_user").convertTo[String],
        fields("fast_password").convertTo[String],
        fields("fast_query").convertTo[String],
        fields("fast_main_query").convertTo[String],
        fields("fast_sc_query").convertTo[String],
        fields("fast_agg_query").convertTo[String],
        fields("fast_nni_query").convertTo[String],
        fields("fast_parquet_path").convertTo[String],
        fields("fast_main_parquet_path").convertTo[String],
        fields("fast_sc_parquet_path").convertTo[String],
        fields("fast_agg_parquet_path").convertTo[String],
        fields("fast_nni_parquet_path").convertTo[String],
        fields("elastic_nodes").convertTo[String],
        fields("elastic_port").convertTo[String],
        fields("elastic_user").convertTo[String],
        fields("elastic_password").convertTo[String])




def readFileSchema(file, schema, spark):
    s3File = spark.sqlContext.read \
        .option("delimiter", "|") \
        .option("quote", "") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .option("mode", "DROPMALFORMED") \
        .option("mode", "PERMISSIVE") \
        .schema(schema) \
        .csv(file)
    return s3File


def readFile(file, spark):
    s3File = spark.sqlContext.read \
        .option("delimiter", "|") \
        .option("ignoreLeadingWhiteSpace", "true") \
        .option("ignoreTrailingWhiteSpace", "true") \
        .csv(file)
    return s3File


def readJsonFile(jsonFile, spark):
    s3JsonFile = spark.sqlContext.read \
        .option("multiline", "true") \
        .json(jsonFile)
    return s3JsonFile


def readZipFile(file, spark):
    return spark.read.text(file)
