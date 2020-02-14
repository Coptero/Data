from pyspark.sql import *


class IODsl:
    '''
      /**
        * Read table from Hive
        *
        * @param table hive table
        * @param spark SparkSession
        * @return Dataset of ..
        */
    '''

    def readTableHive(table, spark):
        return spark.sqlContext.read.table(table.getTableName)


class IOCopteroInsert:
    def writeToHive(df, table, spark):
        fields = df.spark.read.table(table.getTableName).schema.fieldNames


# La linea da error desconocido df.select(fields.head, fields.tail:_*).write.mode(SaveMode.Overwrite).insertInto(table.getTableName)
# Falta por resolver como tratar los objetos, las clases y sus parametros ¿usar singleton?

def toCopteroInsert(clients):
    IOCopteroInsert(clients)
