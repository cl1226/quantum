package org.excitinglab.quantum.spark.core.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.CheckResult
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseStaticInput

import java.util.Properties

class Jdbc extends BaseStaticInput {

  var config: Config = ConfigFactory.empty()

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    jdbcReader(spark, "com.mysql.cj.jdbc.Driver")
  }

  def jdbcReader(sparkSession: SparkSession, driver: String): Dataset[Row] = {

    var dataframe: Dataset[Row] = null
    val tuple = initProp(sparkSession, driver)
    if (tuple._2 != null && tuple._2.length > 0) {
      tuple._2.map(x => {
        println(s"\t$x")
      })
      dataframe = sparkSession.read.jdbc(config.getString("url"), config.getString("tableName"), tuple._2, tuple._1)
    } else {
      dataframe = sparkSession.read.jdbc(config.getString("url"), config.getString("tableName"), tuple._1)
    }
    dataframe
  }

  def initProp(spark: SparkSession, driver: String): Tuple2[Properties, Array[String]] = {
    val prop = new Properties()
    prop.setProperty("driver", driver)
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    (prop, new Array[String](0))
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
  override def checkConfig(): CheckResult = CheckResult.success()
}
