from pyspark import *
import pyspark.sql.functions as F


class Utils:

    def getDatesWhereClause(startDate, endDate):
        def dayIterator(start, end):
            Iterator.iterate(end)(_minusDays1) takeWhile(_isAfterstart)

        dayIterator(startDate, endDate).map{date = > new StringBuilder("(year = ")
            .append(date.getYear)
            .append(" and month = ")
            .append(date.getMonthOfYear)
            .append(" and day = ")
            .append(date.getDayOfMonth)
            .append(")")
            .result
        }.mkString("(", " or ", ")")
