import sys

folderPath = "C:/Users/mou_i/Desktop/Python/LabCoptero/"
sys.path.append(folderPath)
from pyspark import *
import pyspark.sql.functions as F
from datetime import date, datetime, timedelta
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StringType
# from config.CopteroConfig import CopteroConfig
from model.Network import Network


class Utils:

    def getDatesWhereClause(startDate, endDate):
        delta = timedelta(days=1)
        starBracket = "("
        endBracket = ")"
        dayIt = starBracket + ""
        while startDate <= endDate:
            dayIt = dayIt + starBracket + startDate.strftime("year = %Y and month = %m and day = %d") + endBracket
            dayIt = dayIt + " or "
            print(dayIt)
            startDate += delta

        dayIt = dayIt + endBracket
        return dayIt

    def getLocalDateFromString(strDate):
        return datetime.strptime(strDate, "%Y%m%d")

    def addPartitionsFields(clients, conf):
        clients. \
            withColumn("year", F.lit(conf.inputDate.year)). \
            withColumn("month", F.lit(conf.inputDate.month)). \
            withColumn("day", F.lit(conf.inputDate.day))
        return clients

    def dropColumnsDuplicatesByJoin(clients, conf):
        clients.drop("id1", "id2")
        return clients

    def fillEmptyFastColumns(df):
        df.na.fill(Constants.EMPTY_STRING, [
            "fast_customer",
            "fast_end_customer",
            "fast_country",
            "fast_address",
            "mrc",
            "fast_currency",
            "fast_name",
            "fast_service_id",
            "fast_product_code",
            "fast_service_type",
            "fast_activated_when",
            "fast_access_diversity",
            "fast_max_resolution_time",
            "fast_service_availability",
            "fast_provision_time"])
        return df

    def validateNum(field):
        try:
            int(field)
            return field
        except:
            return Constants.EMPTY_STRING

    validateNumeric = F.udf(validateNum, StringType())

    def kibana_country(fastCountry):
        kc = Constants.fastCountries2kibana.get(fastCountry)
        if any(kc):
            return kc
        else:
            return ""

    kibanaCountry = F.udf(kibana_country, StringType())

    def smc_ClusterFromGroup(supportGroup):
        smc = Constants.smcCluster.get(supportGroup)
        if any(smc):
            return smc
        else:
            return ""

    smcClusterFromGroup = F.udf(smc_ClusterFromGroup, StringType())

    def concatTwoColums(c1, c2):
        return c1 + Constants.CONCAT + c2

    concat2Columns = F.udf(concatTwoColums, StringType())

    def concatThreeColums(c1, c2, c3):
        return c1 + Constants.CONCAT + c2 + Constants.CONCAT + c3

    concat3Columns = F.udf(concatThreeColums, StringType())

    def urlWhiteSpaces(field: str):
        if field is None:
            return field
        else:
            return field.replace(" ", "%20")

    urlWhitespaces = F.udf(urlWhiteSpaces, StringType())

    def network_NestedObject(customer, endCustomer, routerInterfaceVendor):
        result = []
        # Do we need rest of cases?
        if routerInterfaceVendor is None:
            None
        else:
            if Row("router_interface", "vendor"):
                router_interface = Row("router_interface")
                vendor = Row("vendor")
                if Row("ne_carr", "resource"):
                    ne_carr = Row("ne_carr")
                    resource = Row("resource")
                    pop = ""
                    if len(ne_carr) > 5:
                        pop = ne_carr[3, 6]
                    result = result.append(Network(pop, ne_carr, resource, customer, endCustomer, vendor))

        if result.isEmpty:
            return None
        else:
            return result

    networkNestedObject = F.udf(network_NestedObject)

    def mergeTwoArrays(seqA, seqB):
        if (seqA is None) and (seqB is None):
            return None
        else:
            if seqA is None:
                return seqB
            elif seqB is None:
                return seqA
            else:
                seqA.union(seqB)

    mergeArrays = F.udf(mergeTwoArrays, StringType())

    def getIndexPart(ticketId):
        if len(ticketId) is None:
            return Constants.INDEX_PARTITION_SIZE
        else:
            if len(ticketId) < 4:
                return Constants.INDEX_PARTITION_SIZE
            else:
                try:
                    aux = int(ticketId[3:])
                    return ((abs(aux - 1) / Constants.INDEX_PARTITION_SIZE) + 1) * Constants.INDEX_PARTITION_SIZE
                except:
                    return Constants.INDEX_PARTITION_SIZE

    getIndexPartition = F.udf(getIndexPart, StringType())


class Constants:
    INDEX_PARTITION_SIZE = 500000
    CONCAT = " @@ "
    OPEN_YES = "y"
    OPEN_NO = "n"
    EMPTY_STRING = ""
    EMPTY_STRING_ARRAY = []

    # TODO COMMON ELASTIC INDEX FIELD NAMES
    COUNTRY = "ci_country"
    ASSIGNED = "Assigned"
    IN_PROGRESS = "In Progress"
    # TODO ctes

    openStatus = [ASSIGNED,
                  IN_PROGRESS,
                  "New",
                  "Pending",
                  "Under Review",
                  "Under Investigation",
                  "Draft",
                  "Request For Authorization",
                  "Request For Change",
                  "Planning In Progress",
                  "Scheduled For Review",
                  "Scheduled For Approval",
                  "Scheduled",
                  "Implementation In Progress"]

    notOpenStatus = ["Cancelled",
                     "Closed",
                     "Resolved",
                     "Completed",
                     "Rejected"]

    fastCountries2kibana = {
        "Bahamas, The": "Bahamas",
        "Brunei": "Brunei Darussalam",
        "Congo, Democratic Republic of the": "Democratic Republic of the Congo",
        "Congo, Republic of the": "Congo",
        "Curazao": "Curaçao",
        "East Timor": "Timor-Leste",
        "Faeroe Islands": "Faroe Islands",
        "Falkland Islands (Islas Malvinas)": "Falkland Islands (Malvinas)",
        "Gambia, The": "Gambia",
        "Iran": "Iran (Islamic Republic of)",
        "Korea, North": "Korea, Republic of",
        "Korea, South": "Korea, Democratic People's Republic of",
        "Laos": "Lao People's Democratic Republic",
        "Libyan Arab": "Libyan Arab Jamahiriya",
        "Macao": "Macau",
        "Man, Isle of": "Isle of Man",
        "Moldova, Republic of": "Republic of Moldova",
        "Palestinian Authority": "Palestine",
        "Republic of Ireland": "Ireland",
        "RUSSIAN FEDERATION": "Russia",
        "Serbia and Montenegro": "Serbia",
        "Tanzania, United Republic of": "United Republic of Tanzania",
        "Vatican City State (Holy See)": "Holy See (Vatican City)",
        "Wallis and Futuna": "Wallis and Futuna Islands"}

    # mapping smc_cluster from assigned_support_group

    smcCluster = {
        "CLUSTER_EMEA_L0_ALDI": "ALDI",
        "CLUSTER_EMEA_L1_ALDI": "ALDI",
        "CLUSTER_EMEA_L2_ALDI": "ALDI",
        "CLUSTER_EMEA_L0_ALLIANZ": "ALLIANZ",
        "CLUSTER_EMEA_L1_ALLIANZ": "ALLIANZ",
        "CLUSTER_AMERICAS_L0_AVIANCA": "AVIACA",
        "CLUSTER_EMEA_L0_BBVA": "BBVA",
        "CLUSTER_EMEA_L1_BBVA": "BBVA",
        "CLUSTER_EMEA_L0_CEMEX": "CEMEX",
        "CLUSTER_EMEA_L1_CEMEX": "CEMEX",
        "SMC Miami L1 Technical NSN Operators": "NOKIA",
        "SMC Miami L1 Technical NSN Voice": "NOKIA",
        "SMC Miami L2 Technical NSN Operators": "NOKIA",
        "AO_EMEA": "SMC EMEA",
        "CLUSTER_EMEA_L0_TRADING": "SMC EMEA",
        "SD_EMEA_L0": "SMC EMEA",
        "SD_EMEA_L1": "SMC EMEA",
        "SD_EMEA_L2": "SMC EMEA",
        "SD_EMEA_MOBILE": "SMC EMEA",
        "SD_EMEA_WEBCONFERENCING": "SMC EMEA",
        "SD_EMEA_Query": "SMC EMEA",
        "TGS SMC LATAM L2 Analyst": "SMC LATAM",
        "TGS SMC LATAM L1 Technical": "SMC LATAM",
        "TGS SMC LATAM L0 Technical HCS": "SMC LATAM",
        "TGS SMC LATAM L1 Technical HCS": "SMC LATAM",
        "TGS SMC LATAM L2 Technical HCS": "SMC LATAM",
        "TGS SMC LATAM L2 DAY AFTER - SR HCS": "SMC LATAM",
        "SD_AMERICAS_QUERY": "SMC MIAMI",
        "SD_AMERICAS_L1": "SMC MIAMI",
        "SD_AMERICAS_L2_TECHNICAL": "SMC MIAMI",
        "SMC Miami L1 Technical GA": "SMC MIAMI",
        "SMC Miami L2 Technical GA": "SMC MIAMI"}
