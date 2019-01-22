package trembita.seamless

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl._
import cats.~>
import trembita.operations.InjectTaggedK
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.concurrent.Future
import scala.reflect.ClassTag

package object akka_spark {
  def akkaToSpark[Mat](bufferLimit: Int)(implicit materializer: Materializer,
                                         spark: SparkSession): InjectTaggedK[Source[?, Mat], λ[β => Future[RDD[β]]]] =
    new InjectTaggedK[Source[?, Mat], λ[β => Future[RDD[β]]]] {
      def apply[A: ClassTag](fa: Source[A, Mat]): Future[RDD[A]] = {
        val rddSink   = Sink.fromGraph(new RDDSink[A](bufferLimit)(spark))
        val futureRDD = fa.toMat(rddSink)(Keep.right).run()
        futureRDD
      }
    }

  implicit val sparkToAkka: InjectTaggedK[RDD, Source[?, NotUsed]] =
    InjectTaggedK.fromArrow[RDD, Source[?, NotUsed]](λ[RDD[?] ~> Source[?, NotUsed]](rdd => RDDSource(rdd)))
}
