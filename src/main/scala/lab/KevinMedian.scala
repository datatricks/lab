package lab

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class KevinMedian extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", IntegerType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    Seq(StructField("arr", ArrayType(IntegerType, false)))
  )

  //   StructType(Seq(StructField("arr", ArrayType(IntegerType, false), false)

  // This is the output type of your aggregatation function.
  override def dataType: DataType = IntegerType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array(0)
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Seq[IntegerType]](0) ++ Array(input.getInt(0))
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //     buffer1(0) = buffer1.getAs[Long](0) ++ buffer2.getAs[Long](0)
    buffer1(0) = buffer1.getAs[Seq[IntegerType]](0) ++ buffer2.getAs[Seq[IntegerType]](0)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    //     row.getAs[Seq[String]]("result") if return is any, all variable must be explicitly defined?
    //     val sorted = buffer.getAs[Seq[Integer]]("arr").sorted
    val sorted = buffer.getAs[Seq[Integer]](0)
    val length = sorted.length
    var median = sorted(length/2)
    //     val median = sorted(length/2)
  }
}