package org.excitinglab.quantum.spark.core.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseStaticInput

class Hive extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    spark.sql(config.getString("pre_sql"))
  }

  /**
   * Set Config.
   * */
  override def setConfig(config: Config): Unit = {
    this.config = config
  }

  /**
   * Get Config.
   * */
  override def getConfig(): Config = this.config

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "pre_sql")
  }
}
