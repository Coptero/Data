import sys

sys.path.append("C:/Users/mou_i/Desktop/Python/LabCoptero/")
from pyspark import *
import pyspark.sql.functions as F
from datetime import date, datetime, timedelta
from pyspark.sql import DataFrame, Row
from pyspark.sql.types import StringType
import json
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
            "fast_provision_time",
            "fast_top_object_id",
            "fast_top_object_id"
            "fast_prod_spec"
        ])
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
        if kc is None:
            return fastCountry
        else:
            return kc

    kibanaCountry = F.udf(kibana_country, StringType())

    def empty_EndCustomerCorrect(customer_correct, end_customer_correct):
        if end_customer_correct is None:
            return customer_correct
        elif end_customer_correct == "":
            return customer_correct
        else:
            return end_customer_correct

    emptyEndCustomerCorrect = F.udf(empty_EndCustomerCorrect, StringType())

    def smc_ClusterFromGroup(supportGroup):
        smc = Constants.smcCluster.get(supportGroup)
        if smc is None:
            return ""
        else:
            return smc

    smcClusterFromGroup = F.udf(smc_ClusterFromGroup, StringType())

    def concatTwoColums(c1, c2):
        return str(c1) + Constants.CONCAT + str(c2)

    concat2Columns = F.udf(concatTwoColums, StringType())

    def concatThreeColums(c1, c2, c3):
        return str(c1) + Constants.CONCAT + str(c2) + Constants.CONCAT + str(c3)

    concat3Columns = F.udf(concatThreeColums, StringType())

    def urlWhiteSpaces(field):
        if field is None:
            return field
        else:
            return field.replace(" ", "%20")

    urlWhitespaces = F.udf(urlWhiteSpaces, StringType())

    def get_CircuitID(ci_name):
        if ci_name is None:
            return ci_name
        else:
            if ci_name.find("]- ID:EL"):
                sub = ci_name[ci_name.find("]- ID:EL") + 8:]
                return sub[0: sub.find("_")]
            elif ci_name.find("]- ID:IC"):
                sub = ci_name[ci_name.find("]- ID:IC") + 8:]
                return sub[0: sub.find("_")]
            else:
                return None

    getCircuitID = F.udf(get_CircuitID, StringType())

    def network_NestedObject(customer, endCustomer, routerInterfaceVendorTypeSet):
        if endCustomer is None:
            endCustomer = ""
        result = []
        if routerInterfaceVendorTypeSet is not None:
            row_rivt = routerInterfaceVendorTypeSet[0].__getitem__("router_interface_vendor_type")
            if row_rivt is not None:
                for i in range(len(row_rivt)):
                    router_interface = row_rivt[i].__getitem__("router_interface")
                    access_type = row_rivt[i].__getitem__("access_type")
                    vendor = row_rivt[i].__getitem__("vendor")
                    router_interface = row_rivt[i].__getitem__("router_interface")
                    ne_carr = router_interface[0].__getitem__("ne_carr")
                    resource = router_interface[0].__getitem__("resource")
                    # Do we need rest of cases?
                    if router_interface is not None and vendor is not None and access_type is not None:
                        if ne_carr is not None and resource is not None:
                            pop = ""
                            if len(ne_carr) > 5:
                                pop = ne_carr[3: 6]
                            interface = resource
                            subInterface = ""
                            if resource.startswith("Loopback"):
                                interface = "Loopback"
                                subInterface = resource[8:]
                            elif resource.startswith("Tunnel"):
                                interface = "Tunnel"
                                subInterface = resource[6:]
                            elif resource.find("_") and resource.find("."):
                                interface = resource[resource.find("_") + 1: resource.find(".")]
                                subInterface = resource[resource.find("_") + 1:]
                            network = Network(pop, ne_carr, access_type, endCustomer, subInterface, vendor, interface,
                                              customer)
                            result.append(vars(network))

        if result is not None:
            return json.dumps(result)[1:-1]
        else:
            return None

    networkNestedObject = F.udf(network_NestedObject)

    def string_ToArray(stringToSplit):
        seq = stringToSplit.split(",")
        return list(dict.fromkeys(seq))

    stringToArray = F.udf(string_ToArray)

    def mergeTwoArrays(seqA, seqB):
        if (seqA is None) and (seqB is None):
            return None
        else:
            if seqA is None:
                return seqB
            elif seqB is None:
                return seqA
            else:
                # return seqA.union(seqB)
                return seqA + seqB  # no sabemos si esto es correcto

    mergeArrays = F.udf(mergeTwoArrays, StringType())

    def add_ToArray(element, seq):
        if element == None and seq == None:
            return None
        elif element == None and seq != None:
            return seq
        elif element == "" and seq != None:
            return seq
        elif element != None and seq == None:
            return element
        elif element != None and seq == [""]:
            return element
        else:
            seq.insert(0, element)
            seq_str = ', '.join(seq)
            return seq_str

    addToArray = F.udf(add_ToArray)

    def get_WorkInfoCategory(workInfoNotes):
        if workInfoNotes is None:
            return ""
        else:
            if workInfoNotes.startswith("Autodiagnóstico - WorkInfo 2"):
                return "Info FAST"
            elif workInfoNotes.startswith("Autodiagnóstico - WorkInfo 1"):
                return "Auto diagnosis"
            elif workInfoNotes.startswith("Subject:Incident Bulletin"):
                return "Bulletin"
            else:
                return ""

    getWorkInfoCategory = F.udf(get_WorkInfoCategory, StringType())

    def getIndexPart(ticketId):
        if len(ticketId) is None:
            return Constants.INDEX_PARTITION_SIZE
        else:
            if len(ticketId) < 4:
                return Constants.INDEX_PARTITION_SIZE
            else:
                try:
                    aux = int(ticketId[3:])
                    return (int((abs(aux - 1) / Constants.INDEX_PARTITION_SIZE) + 1)) * Constants.INDEX_PARTITION_SIZE
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
