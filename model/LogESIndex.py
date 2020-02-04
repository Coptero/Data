from dataclasses import dataclass
from datetime import datetime


# no estoy seguro de que esto sea así
@dataclass
class LogESIndex(object):
    def __init__(self):
        self.file: str = None
        self.count: int = None
        self.success: bool = None
        self.exception: str = None,
        self.start_date: str = None,
        self.end_date: str = None

    def startLogStatus(self, file):
        self.file = file
        self.start_date = datetime.today().strftime('%Y%m%d%H%M%S')
