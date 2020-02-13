from dataclasses import dataclass


@dataclass
class Table(object):
    database: str
    table: str

    def getTableName(self):
        return self.database +"."+ self.table
