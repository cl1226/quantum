package org.excitinglab.quantum.spark.redis.partitioner

import org.apache.spark.Partition
import org.excitinglab.quantum.spark.redis.RedisConfig

case class RedisPartition(index: Int,
                          redisConfig: RedisConfig,
                          slots: (Int, Int)) extends Partition
