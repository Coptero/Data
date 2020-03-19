import logging
import sys

sys.path.append("C:/Users/gonza/Downloads/Data-master/")
from model.AdminNumberTags import AdminNumberTags
from model.FastByAdmin import FastByAdmin

from model.OperatingTags import OperatingTags
from model.OperationalManager import OperationalManager
from model.TicketImpact import TicketImpact
from model.TicketPriority import TicketPriority
from model.TicketReportedSource import TicketReportedSource
from model.TicketStatus import TicketStatus
from model.TicketSubstatus import TicketSubstatus
from model.TicketUrgency import TicketUrgency
from pyspark.sql import *
import pyspark.sql.functions as F
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.DynamoDBDsl import DynamoDBDsl
from utils.Utils import Constants, Utils



class RemedyDsl():

    def buildESIndex(detailType, detail, s3confPath, s3filePath, spark):
        sqlContext = SQLContext(spark)
        # TODO.confinsteadof.json        val
        confJson = S3FilesDsl.readConfigJson(s3confPath)

        rodTicketANTags = AdminNumberTags.antagsColumns(
            S3FilesDsl.readFile(confJson.tags_admin_path, spark), spark)

        parquetPath = confJson.fast_parquet_path
        rodPostgreAdminNumber = sqlContext.read.parquet(parquetPath)

        logging.info("FAST joins..")
        networkFast = sqlContext.read.parquet(confJson.fast_network_parquet_path)

        logging.info("common joins..")

        # TODO: añadir import de utils.constantes
        # TODO: comprobar parametros que se pasan a los metodos de Utils
        common3 = joinMasterEntities(detail, spark)
        common2 = common3.join(rodPostgreAdminNumber, ["admin_number"], "left")
        common1 = Utils.fillEmptyFastColumns(common2)
        common = common1.join(networkFast, ["admin_number"], "left"). \
            withColumn("networkinfo", Utils.networkNestedObject("fast_customer", "fast_end_customer",
                                                                "router_interface_vendor_type_set")). \
            drop("router_interface_vendor_type_set"). \
            join(rodTicketANTags, ["admin_number"], "left"). \
            withColumn("open", F.when(common1.status_desc.isin(Constants.openStatus), Constants.OPEN_YES).
                       otherwise(F.when(common1.status_desc.isin(Constants.notOpenStatus), Constants.OPEN_NO).
                                 otherwise(Constants.EMPTY_STRING))). \
            withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id")). \
            withColumn("admin_number_escaped", Utils.urlWhitespaces("admin_number")). \
            withColumn("fast_max_resolution_time", Utils.validateNumeric("fast_max_resolution_time")). \
            withColumn("file", F.lit(s3filePath)). \
            fillna(Constants.EMPTY_STRING, ["assigned_agent"])

        if detailType == "helpdesk":
            rodTicketReportedSource = getReportedSource(spark)
            operationalManager = getOperationalManager(confJson.operational_path, spark)
            opTags = OperatingTags.operatingTagsColumns(S3FilesDsl.readFile(confJson.tags_operating_path, spark))
            customer = Customer.customerColumns(S3FilesDsl.readFile(confJson.customer_path, spark))
            endCustomer = endCustomer.endCustomerColumns(S3FilesDsl.readFile(confJson.end_customer_path, spark))

            index1 = common \
                .join(rodTicketReportedSource, ["reported_source_id"], "left") \
                .drop("reported_source_id") \
                .join(operationalManager, ["operating_company_name", "operating_le"], "left") \
                .na.fill(Constants.EMPTY_STRING, ["operational_manager"]) \
                .join(opTags, ["operating_company_name", "operating_le"], "left") \
                .withColumn("tags", Utils.mergeArrays("tags", "operating_tags")) \
                .drop("operating_tags") \
                .join(customer, ["operating_company_name"], "left") \
                .fillna(Constants.EMPTY_STRING, ["customer_correct"]) \
                .join(endCustomer, ["operating_le"], "left") \
                .fillna(Constants.EMPTY_STRING, ["end_customer_correct"]) \
                .withColumn("end_customer_correct",
                            Utils.emptyEndCustomerCorrect("customer_correct", "end_customer_correct")) \
                .withColumn("ci_country", Utils.kibanaCountry("ci_country")) \
                .withColumn("end_user_country", Utils.kibanaCountry("end_user_country")) \
                .withColumn("smc_cluster", Utils.smcClusterFromGroup("assigned_support_group")) \
                .withColumn("ci_name_escaped", Utils.urlWhitespaces("ci_name")) \
                .withColumn("product_categorization_all_tiers",
                            Utils.concat3Columns("product_categorization_tier_1", "product_categorization_tier_2",
                                                 "product_categorization_tier_3")) \
                .withColumn("closure_categorization_all_tiers",
                            Utils.concat3Columns("closure_categorization_tier_1", "closure_categorization_tier_2",
                                                 "closure_categorization_tier_3")) \
                .withColumn("operational_categorization_all_tiers",
                            Utils.concat3Columns("operational_categorization_tier_1",
                                                 "operational_categorization_tier_2",
                                                 "operational_categorization_tier_3")) \
                .withColumnRenamed("reported_source_desc", "reported_source_id")

            index = fastCircuitFields(index1, confJson, spark)

        elif detailType == "problems":
            index1 = common \
                .withColumn("ci_country", Utils.kibanaCountry("ci_country")) \
                .withColumn("ci_name_escaped", Utils.urlWhitespaces("ci_name"))
            index = fastCircuitFields(index1, confJson, spark)

        elif detailType == "changes":
            rodTicketReportedSource = getReportedSource(spark)
            index = common \
                .join(rodTicketReportedSource, ["reported_source_id"], "left") \
                .drop("reported_source_id") \
                .withColumn("ci_country", Utils.kibanaCountry("ci_country")) \
                .withColumn("company_country", Utils.kibanaCountry("company_country")) \
                .withColumnRenamed("reported_source_desc", "reported_source_id")

        # EL USUARIO SOLICITA QUE LAS DESCRIPCIONES DE LOS MAESTROS SE RENOMBREN COMO _id
        indexRenamed = index \
            .withColumnRenamed("status_desc", "status_id") \
            .withColumnRenamed("substatus_desc", "substatus_id") \
            .withColumnRenamed("urgency_desc", "urgency_id") \
            .withColumnRenamed("priority_desc", "priority_id") \
            .withColumnRenamed("impact_desc", "impact_id")

        return indexRenamed


def getReportedSource(spark):
    fileReportedSource = S3FilesDsl.readFile(
        "C:/Users/gonza/Downloads/PruebasLAB/resources/RD_TR_20190909_TICKET_REPORTED_SOURCE.txt", spark)
    return TicketReportedSource.reportedSourceColumns(fileReportedSource)


def getOperationalManager(s3path, spark):
    fileOperationalManager = S3FilesDsl.readFile(s3path, spark)
    return OperationalManager.operationalManagerColumns(fileOperationalManager)


def joinMasterEntities(df, spark):
    fileStatus = S3FilesDsl.readFile("C:/Users/gonza/Downloads/PruebasLAB/resources/RD_TR_20190909_TICKET_STATUS.txt",
                                     spark)
    fileSubstatus = S3FilesDsl.readFile(
        "C:/Users/gonza/Downloads/PruebasLAB/resources/RD_TR_20190923_TICKET_SUBSTATUS.txt", spark)
    fileUrgency = S3FilesDsl.readFile("C:/Users/gonza/Downloads/PruebasLAB/resources/RD_TR_20190909_TICKET_URGENCY.txt",
                                      spark)
    filePriority = S3FilesDsl.readFile(
        "C:/Users/gonza/Downloads/PruebasLAB/resources/RD_TR_20190909_TICKET_PRIORITY.txt", spark)
    fileImpact = S3FilesDsl.readFile("C:/Users/gonza/Downloads/PruebasLAB/resources/RD_TR_20190909_TICKET_IMPACT.txt",
                                     spark)
    rodTicketStatus = TicketStatus.statusColumns(fileStatus)
    rodTicketSubstatus = TicketSubstatus.substatusColumns(fileSubstatus)
    rodTicketUrgency = TicketUrgency.urgencyColumns(fileUrgency)
    rodTicketPriority = TicketPriority.priorityColumns(filePriority)
    rodTicketImpact = TicketImpact.impactColumns(fileImpact)

    df2 = df.join(rodTicketStatus, ["status_id"], "left"). \
        join(rodTicketSubstatus, ["substatus_id", "status_id"], "left"). \
        drop("status_id"). \
        drop("substatus_id"). \
        join(rodTicketUrgency, ["urgency_id"], "left"). \
        drop("urgency_id"). \
        join(rodTicketPriority, ["priority_id"], "left"). \
        drop("priority_id"). \
        join(rodTicketImpact, ["impact_id"], "left"). \
        drop("impact_id")

    return df2


def persistAgentSmc(esIndex, s3confPath, spark):
    sqlContext = SQLContext(spark)
    newOrUpdated = esIndex.select("ticket_id", "assigned_agent", "smc_cluster", "reported_source_id").distinct()
    auxOuterJoin = newOrUpdated.select("ticket_id")
    total = None

    try:
        total = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)
    except Exception as e:
        message = str(e)
        if message.find("Path does not exist"):
            raise e
        else:
            logging.info("catched Path does not exist (first job execution): " + str(e))

            newOrUpdated \
                .repartition(1) \
                .write \
                .mode("overwite") \
                .parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)

            total = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)

    total.cache()

    logging.info("total.count---------------------------------------------" + total.count())

    totalWithoutNewOrUpdated = total \
        .join(auxOuterJoin, ["ticket_id"], "left") \
        .where(auxOuterJoin["ticket_id"].isNull())

    updatedToParquet = totalWithoutNewOrUpdated.unionByName(newOrUpdated)

    updatedToParquet \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)


def fullPersistAgentSmc(esIndex, s3confPath):
    preload = esIndex.select("ticket_id", "assigned_agent", "smc_cluster", "reported_source_id")

    preload \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)

    logging.info("total.count---------------------------------------------" + preload.count())


def removeClosedAgentSmc(esIndex, s3confPath, spark):
    sqlContext = SQLContext(spark)
    closed = esIndex.select("ticket_id", "assigned_agent", "smc_cluster", "reported_source_id").distinct()

    auxOuterJoin = closed.select("ticket_id")

    total = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)

    total.cache()

    logging.info("totalWithoutClosed.count---------------------------------------------" + total.count())

    totalWithoutClosed = total \
        .join(auxOuterJoin, ["ticket_id"], "left") \
        .where(auxOuterJoin["ticket_id"].isNull())

    totalWithoutClosed \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)

    logging.info("totalWithoutClosed.count---------------------------------------------" + totalWithoutClosed.count())


def getAgentSmcCluster(esIndex, s3confPath, spark):
    sqlContext = SQLContext(spark)

    parquet = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)

    esIndex.join(parquet.select("ticket_id", "smc_cluster", "assigned_agent"), ["ticket_id"], "left")


def getRelations(esIndex, s3confPath, spark):
    sqlContext = SQLContext(spark)

    parquet = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_agent_smc_parquet_path)
    agents = parquet \
        .filter(parquet.reported_source_id != parquet.Vendor) \
        .withColumnRenamed("ticket_id", "agent_ticket_id")

    relations = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_relations_parquet_path)

    relatedAgents = relations \
        .join(agents, relations.related_ticket_id == agents.agent_ticket_id, "inner") \
        .groupBy("ticket_id") \
        .agg(F.collect_set("assigned_agent").alias("assignee"),
             F.collect_set("smc_cluster").alias("smc"))

    esIndex \
        .join(relatedAgents, ["ticket_id"], "left") \
        .withColumn("assignee", Utils.addToArray("assigned_agent", "assignee")) \
        .withColumn("smc", Utils.addToArray("smc_cluster", "smc")) \
        .drop("assigned_agent", "smc_cluster")


def persistRelations(esIndexRel, s3confPath, spark):
    sqlContext = SQLContext(spark)

    relationsDF = esIndexRel.select("ticket_id", "related_ticket_id")
    total = None

    try:
        total = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_relations_parquet_path)
    except Exception as e:
        message = str(e)
        if message.find("Path does not exist"):
            raise e
        else:
            logging.info("catched Path does not exist (first job execution): " + str(e))

            relationsDF \
                .repartition(1) \
                .write \
                .mode("overwite") \
                .parquet(S3FilesDsl.readConfigJson(s3confPath).rod_relations_parquet_path)

            total = sqlContext.read.parquet(S3FilesDsl.readConfigJson(s3confPath).rod_relations_parquet_path)

    total.cache()
    logging.info("total.count---------------------------------------------" + total.count())

    total \
        .unionByName(relationsDF) \
        .distinct() \
        .repartition(1) \
        .write \
        .mode("overwrite") \
        .parquet(S3FilesDsl.readConfigJson(s3confPath).rod_relations_parquet_path)