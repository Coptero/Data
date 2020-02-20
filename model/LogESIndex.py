from dataclasses import dataclass
from datetime import datetime


# no estoy seguro de que esto sea así
@dataclass
class LogESIndex(object):
    def __init__(self):
        file: str = None
        count: int = None
        success: bool = None
        exception: str = None
        start_date: str = None
        end_date: str = None

    def startLogStatus(file):
        file = file
        start_date = datetime.now().strftime("%Y%m%d%H%M%S")
