import logging
import sys

folder_path = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folder_path)
from dataclasses import dataclass
from model.AdminNumberTags import AdminNumberTags
from model.FastAggregated import FastAggregated
from model.FastByAdmin import FastByAdmin
from model.FastNNIL2 import FastNNIL2
from model.FastPrincipal import FastPrincipal
from model.FastServiceCircuit import FastServiceCircuit
from model.OperatingTags import OperatingTags
from model.OperationalManager import OperationalManager
from model.TicketImpact import TicketImpact
from model.TicketPriority import TicketPriority
from model.TicketReportedSource import TicketReportedSource
from model.TicketStatus import TicketStatus
from model.TicketSubstatus import TicketSubstatus
from model.TicketUrgency import TicketUrgency
from pyspark.sql import *
from pyspark.sql.functions import udf
import pyspark.sql.functions as F
from Dsl.S3FilesDsl import S3FilesDsl
from Dsl.DynamoDBDsl import DynamoDBDsl
import utils.Utils
from utils.Utils import Constants, Utils

EMPTY_STRING = ""


class RemedyDsl():

    def buildESIndex(detailType, detail, s3confPath, s3filePath, spark):
        sqlContext = SQLContext(spark)
        # TODO.confinsteadof.json        val
        confJson = S3FilesDsl.readConfigJson(s3confPath)

        rodTicketANTags = AdminNumberTags.antagsColumns(
            S3FilesDsl.readFile(confJson.tags_admin_path, spark), spark)
        parquetPath = confJson.fast_parquet_path
        parquetMainPath = confJson.fast_main_parquet_path
        parquetSCPath = confJson.fast_sc_parquet_path
        parquetAggPath = confJson.fast_agg_parquet_path
        parquetNNIPath = confJson.fast_nni_parquet_path
        rodPostgreAdminNumber = sqlContext.read.parquet(parquetPath)
        principalDS = sqlContext.read.parquet(parquetMainPath)
        scDS = sqlContext.read.parquet(parquetSCPath)
        aggDS = sqlContext.read.parquet(parquetAggPath)
        nniDS = sqlContext.read.parquet(parquetNNIPath)

        scOrder2RouterInterfaceDS = scDS. \
            filter((scDS.order_num == "2") & (scDS.sc_id.isNotNull()) & (scDS.ne_carr.isNotNull()) & (
            scDS.resource.isNotNull())). \
            groupBy("sc_id"). \
            agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedFast = principalDS. \
            join(scOrder2RouterInterfaceDS,
                 principalDS.service_circuit == scOrder2RouterInterfaceDS.sc_id, how='left')

        # TODO l2tp e ipsec EN UN SOLO FILTRO
        l2tp_source = joinedFast. \
            filter(joinedFast.l3_acc_cfs_type == "L2TP Logical Access CFS Instance")
        l2tp_source1 = routerInterfaceVendor(l2tp_source, spark)
        l2tp = l2tp_source1.withColumn("l3_acc_cfs_type", F.lit("L2TP"))

        ipsec_source = joinedFast. \
            filter(joinedFast.l3_acc_cfs_type == "IPsec Logical Access CFS Instance")
        ipsec_source1 = routerInterfaceVendor(ipsec_source, spark)
        ipsec = ipsec_source1.withColumn("l3_acc_cfs_type", F.lit("IPsec"))

        joinedScAggOrder2 = scDS. \
            select(F.col("gw_carr_id"), F.col("order_num"), F.col("sc_id")). \
            filter(scDS.order_num == "2"). \
            filter(scDS.gw_carr_id != "none"). \
            join(aggDS, scDS.gw_carr_id == aggDS.nni_group_id, "left"). \
            select(F.col("sc_id"), F.col("gw_carr_id"), F.col("nni_carr").alias("ne_carr"),
                   F.col("nni_resource").alias("resource")). \
            groupBy("sc_id"). \
            agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedFastAgg = principalDS. \
            join(joinedScAggOrder2, principalDS.service_circuit == joinedScAggOrder2.sc_id, "left")

        indirect_source = joinedFastAgg. \
            filter(joinedFastAgg.l3_acc_cfs_type == "Indirect Access CFS Instance")
        indirect_source1 = routerInterfaceVendor(indirect_source, spark)
        indirect = indirect_source1.withColumn("l3_acc_cfs_type", F.lit("Indirect"))

        networkFast = l2tp.unionByName(ipsec).unionByName(indirect)

        logging.info("common joins..")

        # TODO: añadir import de utils.constantes
        # TODO: comprobar parametros que se pasan a los metodos de Utils
        common3 = joinMasterEntities(detail, spark)
        common2 = common3.join(rodPostgreAdminNumber, ["admin_number"], "left")
        common1 = Utils.fillEmptyFastColumns(common2)
        common = common1.join(networkFast, ["admin_number"], "left"). \
            withColumn("network",
                       Utils.networkNestedObject("fast_customer", "fast_end_customer", "router_interface_vendor")). \
            drop("router_interface_vendor"). \
            join(rodTicketANTags, ["admin_number"], "left"). \
            withColumn("ticket_max_value_partition", Utils.getIndexPartition("ticket_id")). \
            withColumn("admin_number_escaped", Utils.urlWhitespaces("admin_number")). \
            withColumn("fast_max_resolution_time", Utils.validateNumeric("fast_max_resolution_time")). \
            withColumn("file", F.lit(s3filePath))

        # withColumn("open", F.when(detail.status_desc.isin(Constants.openStatus), Constants.OPEN_YES).otherwise(
        # F.when(detail.status_desc.isin(Constants.notOpenStatus), Constants.OPEN_NO).otherwise(Constants.EMPTY_STRING))). \

        if detailType == "helpdesk":
            rodTicketReportedSource = getReportedSource(spark)
            operationalManager = getOperationalManager(confJson.operational_path, spark)
            opTags = OperatingTags.operatingTagsColumns(S3FilesDsl.readFile(confJson.tags_operating_path, spark))
            index = common \
                .join(rodTicketReportedSource, ["reported_source_id"], "left") \
                .drop("reported_source_id") \
                .join(operationalManager, ["operating_company_name", "operating_le"], "left") \
                .na.fill(Constants.EMPTY_STRING, ["operational_manager"]) \
                .join(opTags, ["operating_company_name", "operating_le"], "left") \
                .withColumn("tags", Utils.mergeArrays("tags", "operating_tags")) \
                .drop("operating_tags") \
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

        elif detailType == "problems":
            index = common.withColumn("ci_country", Utils.kibanaCountry("ci_country")) \
                .withColumn("ci_name_escaped", Utils.urlWhitespaces("ci_name"))

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
        "C:/Users/mou_i/Desktop/Python/LabCoptero/resources/RD_TR_20190909_TICKET_REPORTED_SOURCE.txt", spark)
    return TicketReportedSource.reportedSourceColumns(fileReportedSource)


def getOperationalManager(s3path, spark):
    fileOperationalManager = S3FilesDsl.readFile(s3path, spark)
    return OperationalManager.operationalManagerColumns(fileOperationalManager)


def joinMasterEntities(df, spark):
    fileStatus = S3FilesDsl.readFile(
        "C:/Users/mou_i/Desktop/Python/LabCoptero/resources/RD_TR_20190909_TICKET_STATUS.txt", spark)
    fileSubstatus = S3FilesDsl.readFile(
        "C:/Users/mou_i/Desktop/Python/LabCoptero/resources/RD_TR_20190923_TICKET_SUBSTATUS.txt", spark)
    fileUrgency = S3FilesDsl.readFile(
        "C:/Users/mou_i/Desktop/Python/LabCoptero/resources/RD_TR_20190909_TICKET_URGENCY.txt", spark)
    filePriority = S3FilesDsl.readFile(
        "C:/Users/mou_i/Desktop/Python/LabCoptero/resources/RD_TR_20190909_TICKET_PRIORITY.txt", spark)
    fileImpact = S3FilesDsl.readFile(
        "C:/Users/mou_i/Desktop/Python/LabCoptero/resources/RD_TR_20190909_TICKET_IMPACT.txt", spark)
    rodTicketStatus = TicketStatus.statusColumns(fileStatus)
    rodTicketSubstatus = TicketSubstatus.substatusColumns(fileSubstatus)
    rodTicketUrgency = TicketUrgency.urgencyColumns(fileUrgency)
    rodTicketPriority = TicketPriority.priorityColumns(filePriority)
    rodTicketImpact = TicketImpact.impactColumns(fileImpact)

    df.join(rodTicketStatus, ["status_id"], "left"). \
        join(rodTicketSubstatus, ["substatus_id", "status_id"], "left"). \
        drop("status_id"). \
        drop("substatus_id"). \
        join(rodTicketUrgency, ["urgency_id"], "left"). \
        drop("urgency_id"). \
        join(rodTicketPriority, ["priority_id"], "left"). \
        drop("priority_id"). \
        join(rodTicketImpact, ["impact_id"], "left"). \
        drop("impact_id")

    print("******************* COMPROBACIÓN ***********************")
    print(df)

    return df


def routerInterfaceVendor(df, spark):
    return df.select(F.col("vpnsite_admin_number").alias("admin_number"), F.col("servsupp_name_txt").alias("vendor"),
                     df.router_interface). \
        filter((F.col("admin_number").isNotNull()) & (F.col("vendor").isNotNull())). \
        groupBy(F.col("admin_number")).agg(
        F.collect_list(F.struct("router_interface", "vendor")).alias("router_interface_vendor"))
