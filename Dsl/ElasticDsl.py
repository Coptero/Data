from datetime import datetime
import sys
import json
import boto3
from pyspark.sql import *


# ---------------------------------------------------------------------------------------------------
# Write into Elastic
# ---------------------------------------------------------------------------------------------------
def toElastic(config, data, idFunction, indexName):
    toElasticRDD(config, data.rdd, idFunction, indexName)


# ---------------------------------------------------------------------------------------------------
# Write into Elastic
# ---------------------------------------------------------------------------------------------------
def toElasticRDD(config, dataRDD, idFunction, indexName):
    data_dic = dataRDD.map(lambda y: y.asDict())
    toES = data_dic.map(idFunction)
    es_write_conf = {
        "es.nodes": config['elastic_nodes'],
        "es.port": config['elastic_port'],
        "es.resource": indexName,
        "es.net.ssl": "false",
        "es.input.json": "yes",
        "es.nodes.wan.only": "false",
        "es.net.http.auth.user": config['elastic_user'],
        "es.net.http.auth.pass": config['elastic_pass']
    }
    toES.saveAsNewAPIHadoopFile(
        path='-',
        outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
        keyClass="org.apache.hadoop.io.NullWritable",
        valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
        conf=es_write_conf
    )


def addId(data):
    # Para insertar un dato en Elastic hay que crear esta estructura: id, doc_a_insertar
    print("DENTRO DE ADDID")
    print("DATOS: ",type(data))
    return (data['InterfaceID'], json.dumps(data.collect()))


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
        print("DATOS: ", index)
        config = {
            "elastic_nodes": "127.0.0.1",
            "elastic_port": "9200",
            "elastic_user": "elastic",
            "elastic_pass": "changeme"
        }
        toElastic(config, index, addId, "cop-lab-test")
        '''index.write.format(
            'org.elasticsearch.spark.sql'
        ).option(
            'es.write.operation', 'index'
        ).option(
            'es.resource', name + datetime.now().strftime("%Y%m")
        ).save()'''

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
            'es.mapping.id', mapId
        ).option(
            'es.resource', name
        ).save()
