from model.FastNNIL2 import nnil2Columns
from model.FastByAdmin import fastColumns
from model.FastPrincipal import principalColumns
from model.FastServiceCircuit import scColumns
from Dsl.S3FilesDsl import readFile

class FastDsl:

    def postgreSQLAdminNumber(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return fastColumns(jdbcDF)

    def postgreSQLMain(url, query, user, password, spark):
        jdbcDF = readFile("s3://rpajares/scalaSparkJobs/exportQueryFASTprincipal.csv")
        return principalColumns(jdbcDF)

    def postgreSQLServiceCircuit(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return scColumns(jdbcDF)

    def postgreSQLAggregated(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return nnil2Columns(jdbcDF)

    def postgreSQL(url, query, user, password, spark):
        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("query", query) \
            .load()
