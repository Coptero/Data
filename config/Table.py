from dataclasses import dataclass


@dataclass
class Table(object):
    database: str
    table: str

    def getTableName(database, table):
        return database +"."+ table
