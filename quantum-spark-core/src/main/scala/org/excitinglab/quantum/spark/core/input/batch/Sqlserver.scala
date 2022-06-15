package org.excitinglab.quantum.spark.core.input.batch

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.{CheckConfigUtil, CheckResult}

import java.util.Properties
import scala.collection.mutable.ArrayBuffer

class Sqlserver extends Jdbc {

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

  override def initProp(spark: SparkSession, driver: String): (Properties, Array[String]) = {
    val prop = new Properties()
    prop.setProperty("driver", driver)
    prop.setProperty("user", config.getString("user"))
    prop.setProperty("password", config.getString("password"))

    // 读取方式：全量(full)/增量(increment)
    val partColumnName = config.getString("partColumnName")
    var predicates: Array[String] = null
    config.getString("readMode") match {
      case "increment" => {
        (prop, predicates)
      }
      case _ => {
        partColumnName match {
          case "" => (prop, new Array[String](0))
          case _ => {
            val numPartitions = config.getInt("numPartitions")
            var predicates: Array[String] = null
            // 取模的方式划分分区, 所以分区字段的选择最好是均匀分布的, 分区的效果比较好
            if (!partColumnName.equals("")) {
              val arr = ArrayBuffer[Int]()
              for(i <- 0 until numPartitions){
                arr.append(i)
              }
              predicates = arr.map(i=>{s"hashbytes('SHA1', $partColumnName)%$numPartitions = $i"}).toArray
            }
            (prop, predicates)
          }
        }
      }
    }
  }

}
