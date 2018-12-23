package com.github.trembita.seamless.akka_spark

import akka.NotUsed
import akka.stream._
import org.apache.spark.sql._
import akka.stream.scaladsl._
import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag
import akka.stream.stage._
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Future, Promise}

class RDDSink[A: ClassTag](bufferLimit: Int)(@transient val spark: SparkSession)
    extends GraphStageWithMaterializedValue[SinkShape[A], Future[RDD[A]]] {
  val in                  = Inlet[A]("rdd.in")
  val shape: SinkShape[A] = SinkShape(in)

  override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[RDD[A]]) = {
    val rddPromise = Promise[RDD[A]]
    val logic = new GraphStageLogic(shape) {
      var rddAcc: RDD[A]        = spark.sparkContext.emptyRDD[A]
      var buffer: ListBuffer[A] = ListBuffer.empty[A]

      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            prependValue(grab(in))
            if (!hasBeenPulled(in)) pull(in)
          }
          override def onUpstreamFinish(): Unit =
            if (buffer.nonEmpty) {
              rddAcc = rddAcc union spark.sparkContext.parallelize(buffer.toList)
              buffer.clear()
              rddPromise.success(rddAcc)
            }
          override def onUpstreamFailure(ex: Throwable): Unit = {
            buffer.clear()
            rddAcc = null
            super.onUpstreamFailure(ex)
          }
        }
      )

      private def prependValue(a: A): Unit = {
        if (buffer.length > bufferLimit) {
          rddAcc = rddAcc union spark.sparkContext.parallelize(buffer.toList)
          buffer.clear()
        }
        buffer += a
      }
    }
    logic -> rddPromise.future
  }
}

object RDDSource {
  def apply[A](rdd: RDD[A]): Source[A, NotUsed] = Source.fromIterator(() => rdd.toLocalIterator)
}
