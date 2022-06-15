package org.excitinglab.quantum.spark.core.transform

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType, quantumDataType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseTransform

class Schema extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val fields = config.getString("fields")
    val structFields = fields.split(",").map(_.trim).map(field => {
      val f = field.split(":")
      StructField(f(0), quantumDataType.fromStructField(f(1).trim.toLowerCase))
    })

    var df1 = df
    val columns = df.columns
    var index = 0
    for (colName <- columns) {
      df1 = df1.withColumn(colName, col(colName).cast(structFields(index).dataType))
      index = index + 1
    }

    val structType = StructType.apply(structFields)
    spark.createDataFrame(df1.rdd, structType)
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
    CheckConfigUtil.checkAllExists(config, "fields")
  }
}
