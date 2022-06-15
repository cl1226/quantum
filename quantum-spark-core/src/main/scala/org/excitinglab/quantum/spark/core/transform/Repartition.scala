package org.excitinglab.quantum.spark.core.transform

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseTransform

class Repartition extends BaseTransform {

  var config: Config = ConfigFactory.empty()

  override def process(spark: SparkSession, df: Dataset[Row]): Dataset[Row] = {
    val newDF = df.repartition(config.getInt("partition_num"))
    newDF
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
    CheckConfigUtil.checkAllExists(config, "partition_num")
  }
}
