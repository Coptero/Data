import datetime
from dataclasses import dataclass


@dataclass
class InputDate(object):
    def __init__(self, year, month, day):
        self.year: str = year
        self.month: str = month
        self.day: str = day

    Pattern = datetime

    def getStringInputDate(self):
        return str(self.year + "" + self.month + "" + self.day)