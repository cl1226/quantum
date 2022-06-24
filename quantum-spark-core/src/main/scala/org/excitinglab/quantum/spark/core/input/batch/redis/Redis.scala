package org.excitinglab.quantum.spark.core.input.batch.redis

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.common.config.CheckResult
import org.excitinglab.quantum.config.{Config, ConfigFactory}
import org.excitinglab.quantum.spark.apis.BaseStaticInput
import org.excitinglab.quantum.spark.core.input.batch.redis.Constants._
import org.excitinglab.quantum.spark.redis.{RedisConfig, RedisEndpoint, toRedisContext}

import scala.collection.JavaConversions._

class Redis extends BaseStaticInput {

  var redisDataType: RedisDataType.Value = _

  var config: Config = ConfigFactory.empty()

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
    CheckResult.success()
  }


  /**
   * Prepare before running, do things like set config default value, add broadcast variable, accumulator.
   */
  override def prepare(spark: SparkSession): Unit = {
    val defaultConfig = ConfigFactory.parseMap(
      Map(
        HOST -> DEFAULT_HOST,
        PORT -> DEFAULT_PORT,
        AUTH -> DEFAULT_AUTH,
        DB_NUM -> DEFAULT_DB_NUM,
        DATA_TYPE -> DEFAULT_DATA_TYPE,
        PARTITION_NUM -> DEFAULT_PARTITION_NUM,
        TIMEOUT -> DEFAULT_TIMEOUT
      ))
    config = config.withFallback(defaultConfig)
  }

  /**
   * Get DataFrame from this Static Input.
   * */
  override def getDataset(spark: SparkSession): Dataset[Row] = {
    val redisConfig = new RedisConfig(RedisEndpoint(
      host = config.getString(HOST),
      port = config.getInt(PORT),
      auth = null,
      dbNum = config.getInt(DB_NUM),
      timeout = config.getInt(TIMEOUT)
    ))

    redisDataType = RedisDataType.withName(config.getString(DATA_TYPE).toUpperCase)
    val keysOrKeyPattern = config.getString(KEYS_OR_KEY_PATTERN)
    val partitionNum = config.getInt(PARTITION_NUM)

    implicit val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    var ds = spark.emptyDataFrame
    redisDataType match {
      case RedisDataType.KV => {
        val resultRDD = sc.fromRedisKV(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
        ds = resultRDD.toDF("raw_key", "raw_message")
      }
      case RedisDataType.HASH =>
        val resultRDD = sc.fromRedisHash(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
        ds = resultRDD.toDF("raw_key", "raw_message")
      case RedisDataType.SET =>
        val resultRDD = sc.fromRedisSet(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
        ds = resultRDD.toDF("raw_message")
      case RedisDataType.ZSET =>
        val resultRDD = sc.fromRedisZSet(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
        ds = resultRDD.toDF("raw_message")
      case RedisDataType.LIST =>
        val resultRDD = sc.fromRedisList(keysOrKeyPattern, partitionNum)(redisConfig = redisConfig)
        ds = resultRDD.toDF("raw_message")
    }
    ds
  }

}
