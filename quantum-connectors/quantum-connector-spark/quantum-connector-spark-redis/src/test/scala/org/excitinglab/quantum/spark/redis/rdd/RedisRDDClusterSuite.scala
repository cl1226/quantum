package org.excitinglab.quantum.spark.redis.rdd

import org.apache.spark.sql.SparkSession
import org.excitinglab.quantum.spark.redis.{RedisConfig, RedisEndpoint, toRedisContext}

object RedisRDDClusterSuite {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("TestRedisRDD")
      .config("redis.host", "127.0.0.1")
      .config("redis.port", 6379)
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("error")

//    val redisConf = new RedisConfig(RedisEndpoint())
//
//    redisConf.hosts.foreach(node => {
//      val jedis = node.connect()
//      jedis.flushAll()
//      jedis.close()
//    })

    val rdd = sc.fromRedisKV("*").sortByKey().collect()

    println(rdd.length)
    rdd.foreach(println)

  }

}
