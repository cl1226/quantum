package org.excitinglab.quantum.spark.core.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}

class Mysql extends Jdbc {

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, config.getString("driver"))
  }

  /**
   * Return true and empty string if config is valid, return false and error message if config is invalid.
   */
  override def checkConfig(): CheckResult = {
    CheckConfigUtil.checkAllExists(config, "driver", "database", "tableName", "user", "password")
  }
}
