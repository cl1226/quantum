package org.excitinglab.quantum.spark.apis

import org.apache.spark.sql.{Dataset, Row, SparkSession}

abstract class BaseStaticInput extends Plugin {

  /**
   * Get DataFrame from this Static Input.
   * */
  def getDataset(spark: SparkSession): Dataset[Row]

}
