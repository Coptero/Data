'''import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

case class InputDate(year: String, month: String, day: String) {

  lazy val Pattern = DateTimeFormat.forPattern("yyyyMMdd")

  def getStringInputDate: String = {
    s"$year$month$day"
  }

  def getLocalInputDate: LocalDate = {
    Pattern.parseLocalDate(getStringInputDate)
  }

  def getStartDate(daysMinus: Int): LocalDate ={
    getLocalInputDate.minusDays(daysMinus)
  }


}
from functools import wraps

def memoize(f):
    @wraps(f)
    def memoized(*args, **kwargs):
        key = (args, tuple(sorted(kwargs.items()))) # make args hashable
        result = memoized._cache.get(key, None)
        if result is None:
            result = f(*args, **kwargs)
            memoized._cache[key] = result
        return result
    memoized._cache = {}
    return memoized
'''

from dataclasses import dataclass
import datetime

@dataclass
class ConfigJson(object):
    year: str
    month: str
    day: str

    @property
    @ memoize
    def my_lazy_val(self):
        return DateTimeFormat.forPattern("yyyyMMdd")


  def getStringInputDate() :
    return "year,month,day" #return strftime("%Y%m%d")

  def getLocalInputDate() :
    return getLocalInputDate.strftime("%Y%m")

#¿