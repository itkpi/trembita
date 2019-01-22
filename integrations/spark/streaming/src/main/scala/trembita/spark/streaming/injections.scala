package trembita.spark.streaming

import trembita.operations.InjectTaggedK
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.language.higherKinds
import scala.collection.mutable
import scala.reflect.ClassTag

trait injections {
  implicit def rdd2DStream(implicit ssc: StreamingContext): InjectTaggedK[RDD, DStream] = new InjectTaggedK[RDD, DStream] {
    def apply[A: ClassTag](fa: RDD[A]): DStream[A] = {
      val sc    = ssc.sparkContext
      val queue = mutable.Queue(fa)
      ssc.queueStream(queue)
    }
  }

  implicit def fromRDD[F[_]](implicit ssc: StreamingContext, inject: InjectTaggedK[F, RDD]): InjectTaggedK[F, DStream] =
    new InjectTaggedK[F, DStream] {
      override def apply[A: ClassTag](fa: F[A]): DStream[A] = {
        val rdd   = inject(fa)
        val queue = mutable.Queue(rdd)
        ssc.queueStream(queue)
      }
    }
}
