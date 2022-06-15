package org.excitinglab.quantum.spark.core.output.batch

import org.apache.spark.sql.{Dataset, Row}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseOutput

import java.util.Properties

class Mysql extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  override def process(df: Dataset[Row]): Unit = {
    val prop = new Properties()
    prop.setProperty("driver", config.getString("driver"))
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    val saveMode = config.hasPath("saveMode") match {
      case true => config.getString("saveMode")
      case _ => "append"
    }
    df.write.mode(saveMode).jdbc(config.getString("url"), config.getString("dbtable"), prop)
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
    CheckConfigUtil.checkAllExists(config, "url", "driver", "dbtable", "user", "password")
  }
}
