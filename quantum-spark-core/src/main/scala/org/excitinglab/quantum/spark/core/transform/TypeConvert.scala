package org.excitinglab.quantum.spark.core.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseTransform

/**
 * TypeConvert 类型转换
 * 主要作用：数据类型转换功能
 */
class TypeConvert extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    var colNames = config.getString("colNames")
    if (colNames.equals("*")) {
      colNames = df.columns.mkString(",")
    }
    val newType = config.getString("newType")

    newType match {
      case "string" => {
        df.select(colNames.split(",").map(_.trim).map(n => col(n).cast(StringType)): _*)
      }
      case "integer" => {
        df.select(colNames.split(",").map(_.trim).map(n => col(n).cast(IntegerType)): _*)
      }
      case "double" => {
        df.select(colNames.split(",").map(_.trim).map(n => col(n).cast(DoubleType)): _*)
      }
      case "float" => {
        df.select(colNames.split(",").map(_.trim).map(n => col(n).cast(FloatType)): _*)
      }
      case "long" => {
        df.select(colNames.split(",").map(_.trim).map(n => col(n).cast(LongType)): _*)
      }
      case "boolean" => {
        df.select(colNames.split(",").map(_.trim).map(n => col(n).cast(BooleanType)): _*)
      }
      case _: String => df
    }
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = this.config = config

  /**
   * Get Config.
   * */
  override def getConfig(): Config = this.config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "colNames", "newType")
  }
}
