import sys

sys.path.append("C:/Users/gonza/Downloads/LABSpark/")
from Dsl.S3FilesDsl import S3FilesDsl
from pyspark.sql import *
import logging
from utils.SparkJob import SparkJob
from Dsl.FastDsl import FastDsl
from utils.CryptUtils import decrypt

class CopteroRODChangesJob(SparkJob):
    def runJob(sparkSession, s3confPath, s3filePath):
        spark = sparkSession
        conf = s3confPath

        logging.info("Start batch CopteroFastJob -------------------------------------")

        confJson = S3FilesDsl.readConfigJson(conf)
        url = confJson.fast_postgresql
        query = confJson.fast_query
        queryMAIN = confJson.fast_main_query
        querySC = confJson.fast_sc_query
        queryAGG = confJson.fast_agg_query
        queryNNI = confJson.fast_nni_query
        queryCRM = confJson.fast_crm_query
        querySupplier = confJson.fast_supplier_query
        queryIspwire = confJson.fast_ispwire_query
        confJson.fast_user = b'gAAAAABegymLKE0IhWWVCBoQ9PysJTCPLiEx1HFmE9xilxmFeIfTMy-bEHwJADmPj78AM81yQ3engmBAWapFQ5ZDgJ1teF597w=='
        user = decrypt(confJson.fast_user)
        confJson.fast_password = b'gAAAAABegymLKE0IhWWVCBoQ9PysJTCPLiEx1HFmE9xilxmFeIfTMy-bEHwJADmPj78AM81yQ3engmBAWapFQ5ZDgJ1teF597w=='
        password = decrypt(confJson.fast_password)
        minResults = confJson.fast_min_results
        parquetPath = confJson.fast_parquet_path
        parquetMAINPath = confJson.fast_main_parquet_path
        parquetSCPath = confJson.fast_sc_parquet_path
        parquetAGGPath = confJson.fast_agg_parquet_path
        parquetNNIPath = confJson.fast_nni_parquet_path
        parquetCRMPath = confJson.fast_crm_parquet_path
        parquetSupplierPath = confJson.fast_supplier_parquet_path
        parquetIspwirePath = confJson.fast_ispwire_parquet_path
        parquetNetworkPath = confJson.fast_network_parquet_path

        rodPostgreAdminNumber = FastDsl.postgreSQLAdminNumber(url, query, user, password, spark)

        if (rodPostgreAdminNumber.count() > minResults):
            rodPostgreAdminNumber \
                .write \
                .mode("overwrite") \
                .parquet(parquetPath)

            logging.info("Parquet overwritten ----------------------------------------------------")
        else:
            logging.info("PostgreSql not available -----------------------------------------------")

        mainDS = FastDsl.postgreSQLMain(url, queryMAIN, user, password, spark)
        mainDS \
            .write \
            .mode("overwrite") \
            .parquet(parquetMAINPath)

        scDS = FastDsl.postgreSQLServiceCircuit(url, querySC, user, password, spark)
        scDS \
            .write \
            .mode("overwrite") \
            .parquet(parquetSCPath)

        aggDS = FastDsl.postgreSQLAggregated(url, queryAGG, user, password, spark)
        aggDS \
            .write \
            .mode("overwrite") \
            .parquet(parquetAGGPath)

        nnil2DS = FastDsl.postgreSQLNNIL2(url, queryNNI, user, password, spark)
        nnil2DS \
            .write \
            .mode("overwrite") \
            .parquet(parquetNNIPath)

        crmDS = FastDsl.postgreSQLCRM(url, queryCRM, user, password, spark)
        crmDS \
            .write \
            .mode("overwrite") \
            .parquet(parquetCRMPath)

        supplierDS = FastDsl.postgreSQLSupplier(url, querySupplier, user, password, spark)
        supplierDS \
            .write \
            .mode("overwrite") \
            .parquet(parquetSupplierPath)

        ispwireDS = FastDsl.postgreSQLIspwire(url, queryIspwire, user, password, spark)
        ispwireDS \
            .write \
            .mode("overwrite") \
            .parquet(parquetIspwirePath)

        networkFast = FastDsl.getNetworkFastNestedObject(confJson, spark)
        networkFast.repartition(1).write.mode("overwrite").parquet(parquetNetworkPath)

        logging.info("PostgreSql not available -----------------------------------------------")
