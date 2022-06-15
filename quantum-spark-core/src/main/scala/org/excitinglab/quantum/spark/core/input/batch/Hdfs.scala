package org.excitinglab.quantum.spark.core.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.CheckResult

class Hdfs extends File {
  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val path = buildPathWithDefaultSchema(config.getString("path"), "hdfs://")
    fileReader(spark, path)
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): CheckResult = super.checkConfig()
}
