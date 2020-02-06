from pyspark.sql import *
from datetime import datetime


class ElasticDsl:
    def writeESLogIndex(index, name):
        index.write.format(
            'org.elasticsearch.spark.sql'
        ).option(
            'es.write.operation', 'index'
        ).option(
            'es.resource', name + datetime.now().strftime("%Y%m%d")
        ).save()

    def writeESCorruptRecordsIndex(index, name):
      index.write.format(
        'org.elasticsearch.spark.sql'
      ).option(
        'es.write.operation', 'index'
      ).option(
        'es.resource', name + datetime.now().strftime("%Y%m")
      ).save()

    def writeESAlertsIndex(index):
      index.write.format(
        'org.elasticsearch.spark.sql'
      ).option(
        'es.write.operation', 'index'
      ).option(
        'es.resource', 'copt-rod-alerts'
      ).save()

    def writeMappedESIndex(index, name, mapId):
      index.write.format(
        'org.elasticsearch.spark.sql'
      ).option(
        'es.mapping.id',  mapId
      ).option(
        'es.resource', name
      ).save()


