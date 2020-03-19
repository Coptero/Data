from pyspark.sql import *
import pyspark.sql.functions as F
from Dsl.S3FilesDsl import S3FilesDsl
from model.AdminNumberTags import AdminNumberTags
from utils.Utils import Utils

class SolarWindsDsl():
    def adminNumberTags(tagsAdminPath):
        rodTicketANTags =  AdminNumberTags.antagsColumns(S3FilesDsl.readFile())

        ticketANTags = rodTicketANTags \
            .groupBy("admin_number") \
            .agg(F.concat_ws(",", F.collect_set("tags").alias("tags"))) \
            .withColumn("tags", Utils.stringToArray("tags"))
        return ticketANTags