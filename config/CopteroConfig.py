from configparser import ConfigParser
from json import JSONDecoder, JSONEncoder

from config import Table, InputDate


class CopteroConfig(JSONEncoder):

    def configToTable(self, config: ConfigParser):
        database = config.read_string("database")
        table = config.read_string("table")
        return Table(database, table)

    rootConfig = ConfigParser().read("coptero.conf")
    sampleEntity: Table = configToTable(rootConfig.__getattribute__("sample.entity"))
    first: int = rootConfig.getint("parameters.first")

    inputDate: InputDate = InputDate(rootConfig.__getattribute__("year"), rootConfig.__getattribute__("month"),
                                     rootConfig.__getattribute__("day"))
