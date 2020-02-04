from dataclasses import dataclass


@dataclass
class OperatingTags(object):
    operating_company_name: str
    operating_le: str
    operating_tags: list

    def operatingTagsColumns(file):
        file.map(lambda r: OperatingTags(r.getString(0), r.getString(1), r.getString(2).split(",")))

'''
package tbs.bigdata.model

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object OperatingTags {

  case class OperatingTags(
                operating_company_name: String,
                operating_le: String,
                operating_tags: Array[String])

  def operatingTagsColumns(file: DataFrame)(implicit spark: SparkSession): Dataset[OperatingTags] ={
    import spark.implicits._
    file.map(r => OperatingTags(r.getString(0), r.getString(1), r.getString(2).split(",")))
  }
}
'''