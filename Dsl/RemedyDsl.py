import logging

from model import AdminNumberTags
import model.FastAggregated
import model.FastByAdmin
import model.FastNNIL2
import model.FastPrincipal
import model.FastServiceCircuit
import model.OperatingTags
import model.OperationalManager
import model.TicketImpact
import model.TicketPriority
import model.TicketReportedSource
import model.TicketStatus
import model.TicketSubstatus
import model.TicketUrgency
import pyspark.sql.functions as F
import pyspark.sql
import Dsl.S3FilesDsl
import Dsl.DynamoDBDsl


class RemedyDsl(logging):

    def buildESIndex(detailType, detail, s3confPath, s3filePath, spark):
        # TODO.confinsteadof.json        val
        confJson = Dsl.S3FilesDsl.S3FilesDsl.readConfigJson(s3confPath)

        rodTicketANTags = AdminNumberTags.AdminNumberTags.antagsColumns(
            Dsl.S3FilesDsl.S3FilesDsl.readFile(confJson.tags_admin_path, spark))
        parquetPath = confJson.fast_parquet_path
        parquetMainPath = confJson.fast_main_parquet_path
        parquetSCPath = confJson.fast_sc_parquet_path
        parquetAggPath = confJson.fast_agg_parquet_path
        parquetNNIPath = confJson.fast_nni_parquet_path
        rodPostgreAdminNumber = spark.read.parquet(parquetPath)
        principalDS = spark.read.parquet(parquetMainPath)
        scDS = spark.read.parquet(parquetSCPath)
        aggDS = spark.read.parquet(parquetAggPath)
        nniDS = spark.read.parquet(parquetNNIPath)

        scOrder2RouterInterfaceDS = scDS. \
            filter("order_num" == "2" & "sc_id".isNotNull & "ne_carr".isNotNull & "resource".isNotNull). \
            groupBy("sc_id"). \
            agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedFast = principalDS. \
            join(scOrder2RouterInterfaceDS,
                 principalDS.col("service_circuit") == scOrder2RouterInterfaceDS.col("sc_id"), "left")

        # TODO l2tp e ipsec EN UN SOLO FILTRO
        l2tp = joinedFast. \
            filter("l3_acc_cfs_type" == "L2TP Logical Access CFS Instance"). \
            transform(routerInterfaceVendor). \
            withColumn("l3_acc_cfs_type", F.lit("L2TP"))

        ipsec = joinedFast. \
            filter("l3_acc_cfs_type" == "IPsec Logical Access CFS Instance"). \
            transform(routerInterfaceVendor). \
            withColumn("l3_acc_cfs_type", F.lit("IPsec"))

        joinedScAggOrder2 = scDS. \
            select("gw_carr_id", "order_num", "sc_id"). \
            filter("order_num" == "2"). \
            filter("gw_carr_id" != "none"). \
            join(aggDS, scDS.col("gw_carr_id") == aggDS.col("nni_group_id"), "left"). \
            select("sc_id", "gw_carr_id", "nni_carr".alias("ne_carr"), "nni_resource".alias("resource")). \
            groupBy("sc_id"). \
            agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedFastAgg = principalDS. \
            join(joinedScAggOrder2, principalDS.col("service_circuit") == joinedScAggOrder2.col("sc_id"), "left")

        indirect = joinedFastAgg. \
            filter("l3_acc_cfs_type" == "Indirect Access CFS Instance"). \
            transform(routerInterfaceVendor). \
            withColumn("l3_acc_cfs_type", F.lit("Indirect"))

        networkFast = l2tp.unionByName(ipsec).unionByName(indirect)

        logging.info("common joins..")

        #TODO: añadir import de utils.constantes
        common = detail.toDF(). \
            transform(joinMasterEntities). \
            join(rodPostgreAdminNumber, F.sequence("admin_number"), "left"). \
            transform(fillEmptyFastColumns). \
            join(networkFast, F.sequence("admin_number"), "left"). \
            withColumn("network", networkNestedObject("fast_customer", "fast_end_customer", "router_interface_vendor")). \
            drop("router_interface_vendor"). \
            join(rodTicketANTags, F.sequence("admin_number"), "left").\
            withColumn("open", F.when("status_desc".isin(Constants.openStatus),Constants.OPEN_YES).otherwise(F.when("status_desc".isin(Constants.notOpenStatus),Constants.OPEN_NO).otherwise(Constants.EMPTY_STRING))). \
            withColumn("ticket_max_value_partition", getIndexPartition("ticket_id")). \
            withColumn("admin_number_escaped", urlWhitespaces("admin_number")). \
            withColumn("fast_max_resolution_time", validateNumeric("fast_max_resolution_time")).withColumn("file", lit(
            s3filePath))



    def getReportedSource(spark):
        fileReportedSource = Dsl.S3FilesDsl.S3FilesDsl.readFile(
            Dsl.DynamoDBDsl.DynamoDBDsl.getAuxTablePath("TICKET_REPORTED_SOURCE"))
        return model.TicketReportedSource.reportedSourceColumns(fileReportedSource)

    def getOperationalManager(s3path, spark):
        fileOperationalManager = Dsl.S3FilesDsl.S3FilesDsl.readFile(s3path)
        return model.OperationalManager.OperationalManager.operationalManagerColumns(fileOperationalManager)

    def joinMasterEntities(df, spark):
        fileStatus = Dsl.S3FilesDsl.S3FilesDsl.readFile(Dsl.DynamoDBDsl.DynamoDBDsl.getAuxTablePath("TICKET_STATUS"))
        fileSubstatus = Dsl.S3FilesDsl.S3FilesDsl.readFile(
            Dsl.DynamoDBDsl.DynamoDBDsl.getAuxTablePath("TICKET_SUBSTATUS"))
        fileUrgency = Dsl.S3FilesDsl.S3FilesDsl.readFile(Dsl.DynamoDBDsl.DynamoDBDsl.getAuxTablePath("TICKET_URGENCY"))
        filePriority = Dsl.S3FilesDsl.S3FilesDsl.readFile(
            Dsl.DynamoDBDsl.DynamoDBDsl.getAuxTablePath("TICKET_PRIORITY"))
        fileImpact = Dsl.S3FilesDsl.S3FilesDsl.readFile(Dsl.DynamoDBDsl.DynamoDBDsl.getAuxTablePath("TICKET_IMPACT"))
        rodTicketStatus = model.TicketStatus.TicketStatus.statusColumns(fileStatus)
        rodTicketSubstatus = model.TicketSubstatus.TicketSubstatus.substatusColumns(fileSubstatus)
        rodTicketUrgency = model.TicketUrgency.TicketUrgency.urgencyColumns(fileUrgency)
        rodTicketPriority = model.TicketPriority.TicketPriority.priorityColumns(filePriority)
        rodTicketImpact = model.TicketImpact.TicketImpact.impactColumns(fileImpact)

        df.join(rodTicketStatus, F.sequence("status_id"), "left"). \
            join(rodTicketSubstatus, F.sequence("substatus_id", "status_id"), "left"). \
            drop("status_id"). \
            drop("substatus_id"). \
            join(rodTicketUrgency, F.sequence("urgency_id"), "left"). \
            drop("urgency_id"). \
            join(rodTicketPriority, F.sequence("priority_id"), "left"). \
            drop("priority_id"). \
            join(rodTicketImpact, F.sequence("impact_id"), "left"). \
            drop("impact_id")
        return df

    def routerInterfaceVendor(df, spark):
        df.select("vpnsite_admin_number".alias("admin_number"), "servsupp_name_txt".alias("vendor"),
                  "router_interface"). \
            filter("admin_number".isNotNull & "vendor".isNotNull). \
            groupBy("admin_number").agg(
            F.collect_list(F.struct("router_interface", "vendor")).alias("router_interface_vendor"))
