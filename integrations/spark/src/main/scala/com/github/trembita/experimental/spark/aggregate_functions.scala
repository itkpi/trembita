package com.github.trembita.experimental.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class ProductAggregate extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("product", DoubleType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = 1L

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit =
    buffer(0) = buffer.getAs[Double](0) * input.getAs[Double](0)

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Double](0) * buffer2.getAs[Double](0)
  }
  override def evaluate(buffer: Row): Any = buffer.getDouble(0)
}

class RandomAggregate(child: Expression) extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("value", child.dataType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("random", child.dataType) :: Nil)

  override def dataType: DataType = child.dataType

  override def deterministic: Boolean = false

  override def initialize(buffer: MutableAggregationBuffer): Unit =
    buffer(0) = null

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) =
      if (scala.util.Random.nextBoolean()) buffer.get(0)
      else input.get(0)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) =
      if (scala.util.Random.nextBoolean()) buffer1.get(0)
      else buffer2.get(0)
  }
  override def evaluate(buffer: Row): Any = buffer.get(0)
}

class RMSAggregate extends UserDefinedAggregateFunction {
  override def inputSchema: StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("sum", DoubleType) :: StructField("count", LongType) :: Nil)

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0
    buffer(1) = 0
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getDouble(0) + math.pow(input.getDouble(0), 2)
    buffer(1) = buffer.getLong(1) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) + buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }
  override def evaluate(buffer: Row): Any = math.sqrt(buffer.getDouble(0) / buffer.getLong(1))
}
