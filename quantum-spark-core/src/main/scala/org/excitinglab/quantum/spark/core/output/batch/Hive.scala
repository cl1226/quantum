package org.excitinglab.quantum.spark.core.output.batch

import java.util
import org.apache.spark.sql.{DataFrameWriter, Dataset, Row}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseOutput

import scala.collection.JavaConversions._

class Hive extends BaseOutput {

  var config: Config = ConfigFactory.empty()

  override def process(df: Dataset[Row]): Unit = {
    val sparkSession = df.sparkSession
    if (config.hasPath("sql")) {
      val sql = config.getString("sql")
      sparkSession.sql(sql)
    } else {
      val resultTableName = config.getString("result_table_name")
      val sinkFrame = if (config.hasPath("sink_columns")) {
        df.selectExpr(config.getString("sink_columns").split(","): _*)
      } else {
        df
      }
      val frameWriter: DataFrameWriter[Row] = if (config.hasPath("save_mode")) {
        sinkFrame.write.mode(config.getString("save_mode"))
      } else {
        sinkFrame.write
      }
      if (config.hasPath("partition_by")) {
        val partitionList: util.List[String] = config.getStringList("partition_by")
        frameWriter.partitionBy(partitionList: _*).saveAsTable(resultTableName)
      } else {
        frameWriter.saveAsTable(resultTableName)
      }
    }
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
    if (config.hasPath("sql")) {
      CheckResult.success()
    } else {
      CheckConfigUtil.checkAllExists(config, "result_table_name")
    }
  }
}
