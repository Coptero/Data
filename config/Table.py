from dataclasses import dataclass


@dataclass
class Table(object):
    database: str
    table: str

    def getTableName(self, database, table):
        self.database = database
        self.table = table
        return self.database + "." + self.table
