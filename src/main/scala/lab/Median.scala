package lab

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object Median extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(StructField("inputColumn", IntegerType) :: Nil)

  def bufferSchema: StructType = {
    StructType(StructField("everything", ArrayType(IntegerType, false)) :: Nil)
  }

  def dataType: DataType = IntegerType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Array[Int]()
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer(0).asInstanceOf[IndexedSeq[Int]] ++ IndexedSeq(input.getInt(0))
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1(0).asInstanceOf[IndexedSeq[Int]] ++ buffer2(0).asInstanceOf[IndexedSeq[Int]]
  }

  def evaluate(buffer: Row): Int = {
    val everything = buffer(0).asInstanceOf[IndexedSeq[Int]].sorted
    val length = everything.length
    if(length%2 ==0) everything(length/2 -1) else everything(length/2)
  }
}
