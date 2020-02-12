from pyspark import *
import pyspark.sql.functions as F
from datetime import date,datetime, timedelta
from pyspark.sql import DataFrame, Row
from config.CopteroConfig import  CopteroConfig


class Utils:

    def getDatesWhereClause(startDate: datetime, endDate: datetime):
        delta = timedelta(days=1)
        starBracket = "("
        endBracket = ")"
        dayIt = starBracket + ""
        while startDate <= endDate:
            dayIt = dayIt + starBracket + startDate.strftime("year = %Y and month = %m and day = %d") + endBracket
            dayIt = dayIt + "or"
            print(dayIt)
            startDate += delta

        dayIt = dayIt + endBracket
        return dayIt

    def getLocalDateFromString(strDate: str):
        return datetime.strptime(strDate,"%Y%m%d")

    def fillEmptyFastColumns(df: DataFrame):
        df.na.fill(Constants.EMPTY_STRING, F.sequence(
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
        "fast_provision_time"))
        return df


class Constants:
    INDEX_PARTITION_SIZE = 500000
    CONCAT = " @@ "
    OPEN_YES = "y"
    OPEN_NO = "n"
    EMPTY_STRING = ""
    EMPTY_STRING_ARRAY = []

    #TODO COMMON ELASTIC INDEX FIELD NAMES
    COUNTRY = "ci_country"
    ASSIGNED = "Assigned"
    IN_PROGRESS = "In Progress"
    #TODO ctes

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
        "Brunei" : "Brunei Darussalam",
        "Congo, Democratic Republic of the" : "Democratic Republic of the Congo",
        "Congo, Republic of the" : "Congo",
        "Curazao" : "Curaçao",
        "East Timor" : "Timor-Leste",
        "Faeroe Islands" : "Faroe Islands",
        "Falkland Islands (Islas Malvinas)" : "Falkland Islands (Malvinas)",
        "Gambia, The" : "Gambia",
        "Iran" : "Iran (Islamic Republic of)",
        "Korea, North" : "Korea, Republic of",
        "Korea, South" : "Korea, Democratic People's Republic of",
        "Laos" : "Lao People's Democratic Republic",
        "Libyan Arab" : "Libyan Arab Jamahiriya",
        "Macao" : "Macau",
        "Man, Isle of" : "Isle of Man",
        "Moldova, Republic of" : "Republic of Moldova",
        "Palestinian Authority" : "Palestine",
        "Republic of Ireland" : "Ireland",
        "RUSSIAN FEDERATION" : "Russia",
        "Serbia and Montenegro" : "Serbia",
        "Tanzania, United Republic of" : "United Republic of Tanzania",
        "Vatican City State (Holy See)" : "Holy See (Vatican City)",
        "Wallis and Futuna" : "Wallis and Futuna Islands"}

    #mapping smc_cluster from assigned_support_group

    smcCluster = {
        "CLUSTER_EMEA_L0_ALDI" : "ALDI",
        "CLUSTER_EMEA_L1_ALDI" : "ALDI",
        "CLUSTER_EMEA_L2_ALDI" : "ALDI",
        "CLUSTER_EMEA_L0_ALLIANZ" : "ALLIANZ",
        "CLUSTER_EMEA_L1_ALLIANZ" : "ALLIANZ",
        "CLUSTER_AMERICAS_L0_AVIANCA" : "AVIACA",
        "CLUSTER_EMEA_L0_BBVA" : "BBVA",
        "CLUSTER_EMEA_L1_BBVA" : "BBVA",
        "CLUSTER_EMEA_L0_CEMEX" : "CEMEX",
        "CLUSTER_EMEA_L1_CEMEX" : "CEMEX",
        "SMC Miami L1 Technical NSN Operators" : "NOKIA",
        "SMC Miami L1 Technical NSN Voice" : "NOKIA",
        "SMC Miami L2 Technical NSN Operators" : "NOKIA",
        "AO_EMEA" : "SMC EMEA",
        "CLUSTER_EMEA_L0_TRADING" : "SMC EMEA",
        "SD_EMEA_L0" : "SMC EMEA",
        "SD_EMEA_L1" : "SMC EMEA",
        "SD_EMEA_L2" : "SMC EMEA",
        "SD_EMEA_MOBILE" : "SMC EMEA",
        "SD_EMEA_WEBCONFERENCING" : "SMC EMEA",
        "SD_EMEA_Query" : "SMC EMEA",
        "TGS SMC LATAM L2 Analyst" : "SMC LATAM",
        "TGS SMC LATAM L1 Technical" : "SMC LATAM",
        "TGS SMC LATAM L0 Technical HCS" : "SMC LATAM",
        "TGS SMC LATAM L1 Technical HCS" : "SMC LATAM",
        "TGS SMC LATAM L2 Technical HCS" : "SMC LATAM",
        "TGS SMC LATAM L2 DAY AFTER - SR HCS" : "SMC LATAM",
        "SD_AMERICAS_QUERY" : "SMC MIAMI",
        "SD_AMERICAS_L1" : "SMC MIAMI",
        "SD_AMERICAS_L2_TECHNICAL" : "SMC MIAMI",
        "SMC Miami L1 Technical GA" : "SMC MIAMI",
        "SMC Miami L2 Technical GA" : "SMC MIAMI"}