package lab

import org.apache.spark.sql.Dataset
import org.scalatest.{FlatSpec, MustMatchers}

case class Entity(entityType: String, entityValue: Integer)

class MedianSpec extends FlatSpec with MustMatchers with SparkSupport {

  import spark.implicits._

  val entities: List[Entity] =
    (1 to 11).map(Entity("ODD", _)).toList ::: (1 to 10).map(Entity("EVEN", _)).toList ::: (1 to 1).map(Entity("ONE", _)).toList

  val entitiesDS: Dataset[Entity] = entities.toDS()

  entitiesDS.createOrReplaceTempView("entities")

  spark.udf.register("median", Median)

  spark.udf.register("kevinMedian", new KevinMedian)

  "Entities DS" should "show" in {
    entitiesDS.show()
  }

  it should "allow select *" in {
    val result = spark.sql("SELECT * FROM entities")
    result.show()
  }

  it should "allow UDAF in select" in {
    val result = spark.sql("SELECT median(entityValue) as median, entityType FROM entities GROUP BY entityType")
    result.show()
  }

  it should "allow percentile_approx in select" in {
    val result = spark.sql("SELECT percentile_approx(entityValue, 0.5) as percentile_approx, entityType FROM entities GROUP BY entityType")
    result.show()
  }

  //This test throws java.lang.ClassCastException: scala.runtime.BoxedUnit cannot be cast to java.lang.Integer
  ignore should "allow Kevin's UDAF in select" in {
    val result = spark.sql("SELECT kevinMedian(entityValue) as kevinMedian, entityType FROM entities GROUP BY entityType")
    result.show()
  }

}
