package org.excitinglab.quantum.spark.core.input.batch.redis

object RedisDataType extends Enumeration {
  def RedisDataType: Value = Value

  val KV: Value = Value("KV")
  val HASH: Value = Value("HASH")
  val LIST: Value = Value("LIST")
  val SET: Value = Value("SET")
  val ZSET: Value = Value("ZSET")
}
