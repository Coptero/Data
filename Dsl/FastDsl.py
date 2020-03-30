from model.FastByAdmin import FastByAdmin
from model.FastPrincipal import FastPrincipal
from model.FastServiceCircuit import FastServiceCircuit
from model.FastAggregated import FastAggregated
from model.FastCRM import FastCRM
from model.FastSupplier import FastSupplier
from model.FastIspwire import FastIspwire
from model.FastNNIL2 import FastNNIL2
from utils.Utils import Utils
import pyspark.sql.functions as F
import logging
from pyspark.sql import *


class FastDsl:

    def postgreSQLAdminNumber(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastByAdmin.fastColumns(jdbcDF)

    def postgreSQLMain(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastPrincipal.principalColumns(jdbcDF)

    def postgreSQLServiceCircuit(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastServiceCircuit.scColumns(jdbcDF)

    def postgreSQLAggregated(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastAggregated.aggColumns(jdbcDF)

    def postgreNNIL2(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastNNIL2.nnil2Columns(jdbcDF)

    def postgreSQLCRM(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastCRM.crmColumns(jdbcDF)

    def postgreSQLSupplier(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastSupplier.supplierColumns(jdbcDF)

    def postgreSQLIspwire(url, query, user, password, spark):
        jdbcDF = FastDsl.postgreSQL(url, query, user, password, spark)
        return FastIspwire.ispwireColumns(jdbcDF)

    def postgreSQL(url, query, user, password, spark):
        return spark.read \
            .format("jdbc") \
            .option("url", url) \
            .option("user", user) \
            .option("password", password) \
            .option("query", query) \
            .load()

    def getCRMFast(confJson, spark):
        sqlContext = SQLContext(spark)
        return sqlContext.read.parquet(confJson.fast_crm_parquet_path)

    def getSupplierFast(confJson, spark):
        sqlContext = SQLContext(spark)
        return sqlContext.read.parquet(confJson.fast_supplier_parquet_path)

    def getIspwireFast(confJson, spark):
        sqlContext = SQLContext(spark)
        return sqlContext.read.parquet(confJson.fast_ispwire_parquet_path)

    def fastCircuitFields(df, confJson, spark):
        sqlContext = SQLContext(spark)
        crmFast = FastDsl.getCRMFast(confJson, spark)
        supplierFast = FastDsl.getSupplierFast(confJson, spark)
        ispwireFast = FastDsl.getIspwireFast(confJson, spark)

        Fastdf = df.withColumn("circuit_id", Utils.getCircuitID("ci_name")) \
            .join(crmFast, ["circuit_id"], "left") \
            .join(supplierFast, ["circuit_id"], "left") \
            .join(ispwireFast, ["circuit_id"], "left") \
            .drop("circuit_id")

        return Fastdf

    def routerInterfaceVendorType(df):
        routerdf = df.select(df["vpnsite_admin_number"].alias("admin_number"),
                             df["servsupp_name_txt"].alias("vendor"),
                             df["l3_acc_cfs_type"].alias("access_type"),
                             "router_interface") \
            .filter(df.admin_number.isNotNull() & df.vendor.isNotNull()) \
            .groupBy("admin_number") \
            .agg(F.collect_set(F.struct("router_interface", "vendor", "access_type")).alias(
            "router_interface_vendor_type"))
        return routerdf

    def getNetworkFastNestedObject(confJson, spark):
        sqlContext = SQLContext(spark)
        parquetMainPath = confJson.fast_main_parquet_path
        parquetSCPath = confJson.fast_sc_parquet_path
        parquetAggPath = confJson.fast_agg_parquet_path
        parquetNNIPath = confJson.fast_nni_parquet_path
        mainDS = sqlContext.read.parquet(parquetMainPath)
        scDS = sqlContext.read.parquet(parquetSCPath)
        aggregatedDS = sqlContext.read.parquet(parquetAggPath)
        nniDS = sqlContext.read.parquet(parquetNNIPath)

        principalDS = mainDS.select("service_circuit", "l3_acc_cfs_type", "vpnsite_admin_number", "servsupp_name_txt")
        scOrder2 = scDS.filter(scDS.order_num == "2").select("sc_id", "ne_carr", "resource", "gw_carr_id",
                                                             "nni_carr_id")
        scOrder3 = scDS.filter(scDS.order_num == "3").select("sc_id", "ne_carr", "resource", "port_resource")
        aggDS = aggregatedDS.select(scDS.nni_group_id, "nni_carr", "nni_resource")
        nniOrder1DS = nniDS.filter(scDS.order_num == "1").select("sc_id", "ne_carr", "port_resource")

        principalDS.cache()
        scOrder2.cache()
        scOrder3.cache()
        aggDS.cache()
        nniOrder1DS.cache()

        logging.info("FAST logical access joins..")
        l2tpORipsec = FastDsl.getLogicalAccess(scOrder2, principalDS)

        logging.info("FAST indirect access joins..")
        indirect = FastDsl.getIndirectAccess(scOrder2, principalDS, aggDS)

        logging.info("FAST direct access joins..")
        directOrder3 = FastDsl.getDirectAccess(scOrder3, principalDS)

        logging.info("FAST direct access NNIL2 joins..")
        directOrder2 = FastDsl.getDirectNNIL2Access(scOrder2, principalDS, nniOrder1DS)

        # group by admin number again after unions cause an admin can contain different types
        networkFast = l2tpORipsec \
            .unionByName(indirect) \
            .unionByName(directOrder2) \
            .unionByName(directOrder3) \
            .groupBy("admin_number") \
            .agg(F.collect_set(F.struct("router_interface_vendor_type")).alias(
            "router_interface_vendor_type_set"))

        return networkFast

    def getLogicalAccess(scOrder2, principalDS):
        scOrder2RouterInterfaceDS = scOrder2 \
            .filter(
            scOrder2.sc_id.isNotNull() & scOrder2.ne_carr.isNotNull() & scOrder2.resource.isNotNull()) \
            .groupBy("sc_id") \
            .agg(
            F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedFast = principalDS.join(scOrder2RouterInterfaceDS,
                                      principalDS.service_circuit == scOrder2RouterInterfaceDS.sc_id, "inner")

        l2tp_ORipsec = joinedFast \
            .filter(
            joinedFast.l3_acc_cfs_type == "L2TP Logical Access CFS Instance" | joinedFast.l3_acc_cfs_type == "IPsec Logical Access CFS Instance")
        l2tpORipsec = FastDsl.routerInterfaceVendorType(l2tp_ORipsec)

        l2tpORipsec.cache()
        return l2tpORipsec

    def getIndirectAccess(scOrder2, principalDS, aggDS):
        auxFiltered = scOrder2.select("gw_carr_id", "sc_id").filter(scOrder2.gw_carr_id != "none")

        joinedScAggOrder2 = auxFiltered.join(aggDS, auxFiltered.gw_carr_id == aggDS.nni_group_id, "inner") \
            .select("sc_id", "gw_carr_id", auxFiltered["nni_carr"].alias("ne_carr"),
                    auxFiltered["nni_resource"].alias("resource")) \
            .groupBy("sc_id") \
            .agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedScAggOrder2.cache()
        joinedFastAgg = principalDS.join(joinedScAggOrder2,
                                         principalDS.service_circuit == joinedScAggOrder2.sc_id,
                                         "inner")

        indirect1 = joinedFastAgg.filter(joinedFastAgg.l3_acc_cfs_type == "Indirect Access CFS Instance")

        indirect = FastDsl.routerInterfaceVendorType(indirect1)
        indirect.cache()

        return indirect

    def getDirectAccess(scOrder3, principalDS):
        scOrder3RouterInterfaceDS = scOrder3.filter(scOrder3.sc_id.isNotNull() & scOrder3.ne_carr.isNotNull())

        auxResource = scOrder3RouterInterfaceDS \
            .filter(scOrder3RouterInterfaceDS.resource.isNotNull()) \
            .drop("port_resource")

        auxPortResource = scOrder3RouterInterfaceDS \
            .filter(scOrder3RouterInterfaceDS.port_resource.isNotNull()) \
            .drop("resource") \
            .withColumnRenamed("port_resource", "resource")

        auxTotal = auxResource \
            .unionByName(auxPortResource) \
            .groupBy("sc_id") \
            .agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedFastOrder3 = principalDS.join(auxTotal, principalDS.service_circuit == auxTotal.sc_id, "inner")

        direct_Order3 = joinedFastOrder3.filter(joinedFastOrder3.l3_acc_cfs_type == "Direct Access CFS Instance")
        directOrder3 = FastDsl.routerInterfaceVendorType(direct_Order3)
        directOrder3.cache()

        return directOrder3

    def getDirectNNIL2Access(scOrder2, principalDS, nniOrder1DS):
        scOrder2IDs = scOrder2.select("nni_carr_id", scOrder2["sc_id"].alias("service_circuit_id"))

        joinedScNnil2 = scOrder2IDs \
            .join(nniOrder1DS, scOrder2IDs.nni_carr_id == nniOrder1DS.sc_id, "inner") \
            .select("service_circuit_id", "ne_carr", scOrder2IDs["port_resource"].alias("resource")) \
            .groupBy("service_circuit_id") \
            .agg(F.collect_list(F.struct("ne_carr", "resource")).alias("router_interface"))

        joinedScNnil2.cache()

        joinedFastOrder2 = principalDS.join(joinedScNnil2,
                                            principalDS.service_circuit == joinedScNnil2.service_circuit_id,
                                            "inner")

        direct_Order2 = joinedFastOrder2.filter(joinedFastOrder2.l3_acc_cfs_type == "Direct Access CFS Instance")
        directOrder2 = FastDsl.routerInterfaceVendorType(direct_Order2)
        directOrder2.cache()

        return directOrder2
