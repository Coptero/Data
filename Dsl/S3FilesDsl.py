import boto3
import json
from config import ConfigJson
from pyspark.sql import *

class S3FilesDsl:
    def readConfigJson(s3confPath):
        with open(s3confPath) as json_file:
            fields = json.load(json_file)


        configCop = ConfigJson.ConfigJson(
            str(fields["operational_path"]),
            str(fields["tags_admin_path"]),
            str(fields["tags_operating_path"]),
            int(fields["fast_min_results"]),
            str(fields["fast_postgresql"]),
            str(fields["fast_user"]),
            str(fields["fast_password"]),
            str(fields["fast_query"]),
            str(fields["fast_main_query"]),
            str(fields["fast_sc_query"]),
            str(fields["fast_agg_query"]),
            str(fields["fast_nni_query"]),
            str(fields["fast_parquet_path"]),
            str(fields["fast_main_parquet_path"]),
            str(fields["fast_sc_parquet_path"]),
            str(fields["fast_agg_parquet_path"]),
            str(fields["fast_nni_parquet_path"]),
            str(fields["elastic_nodes"]),
            str(fields["elastic_port"]),
            str(fields["elastic_user"]),
            str(fields["elastic_password"])
        )

        return configCop

    def readFileSchema(file, schema, spark):
        sqlContext = SQLContext(spark)
        s3File = sqlContext.read \
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
        sqlContext = SQLContext(spark)
        s3File = sqlContext.read \
            .option("delimiter", "|") \
            .option("ignoreLeadingWhiteSpace", "true") \
            .option("ignoreTrailingWhiteSpace", "true") \
            .csv(file)
        return s3File

    def readJsonFile(jsonFile, spark):
        sqlContext = SQLContext(spark)
        s3JsonFile = sqlContext.read \
            .option("multiline", "true") \
            .json(jsonFile)
        return s3JsonFile

    def readZipFile(file, spark):
        return spark.read.text(file)
