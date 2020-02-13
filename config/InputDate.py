from datetime import datetime, timedelta

class InputDate(object):
    year: str
    month: str
    day: str

    def getStringInputDate(self):
        return self.year+self.month+self.day

    def getLocalInputDate(self):
        return  datetime.strptime(self.getStringInputDate, "%Y%m%d")

    def getStartDate(self, daysMinus):
        delta = timedelta(days=daysMinus)
        return self.getLocalInputDate() - delta