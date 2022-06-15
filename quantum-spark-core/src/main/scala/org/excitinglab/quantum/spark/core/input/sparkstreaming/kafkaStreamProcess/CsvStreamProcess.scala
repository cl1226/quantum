package org.excitinglab.quantum.spark.core.input.sparkstreaming.kafkaStreamProcess

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DataTypes, StructField, quantumDataType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.excitinglab.quantum.config.Config
import org.excitinglab.quantum.spark.core.input.sparkstreaming.KafkaStream

class CsvStreamProcess(config: Config) extends KafkaStream {

  override def rdd2dataset(spark: SparkSession, rdd: RDD[ConsumerRecord[String, AnyRef]]): Dataset[Row] = {

    val fields = config.getString("fields")
    val structFields = fields.split(",").map(_.trim).map(field => {
      val f = field.split(":")
      StructField(f(0), quantumDataType.fromStructField(f(1).trim.toLowerCase))
    })

    val schema = DataTypes.createStructType(structFields)

    val transformedRDD = rdd.map(record => {
      record.value().toString
    })

    import spark.implicits._

    val df = spark.read
      .schema(schema)
      .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
      .csv(spark.createDataset(transformedRDD))

    df

  }

}
