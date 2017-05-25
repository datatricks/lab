package lab

import org.apache.spark.sql.SparkSession

trait SparkSupport {
  val spark: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Lab Test Session")
    .getOrCreate()
}
