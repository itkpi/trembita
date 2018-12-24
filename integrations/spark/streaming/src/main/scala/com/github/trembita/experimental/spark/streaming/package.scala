package com.github.trembita.experimental.spark

import cats.effect.IO
import cats.{Id, Monad}
import com.github.trembita.operations.{CanGroupByOrdered, LiftPipeline, MagnetF}
import scala.language.experimental.macros
import scala.language.{higherKinds, implicitConversions}
import com.github.trembita.DataPipelineT
import com.github.trembita.fsm.{FSM, InitialState}
import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql.{AggDecl, AggRes, GroupingCriteria, QueryBuilder, QueryResult}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag

package object streaming extends injections {
  implicit class SparkStreamingOps[F[_], A](val `this`: DataPipelineT[F, A, SparkStreaming])
      extends AnyVal
      with MagnetlessSparkStreamingBasicOps[F, A] {
    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
        queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]
    )(implicit trembitaqlForSparkStreaming: trembitaqlForSparkStreaming[A, G, T, R, Comb],
      run: Spark#Run[F],
      F: Monad[F],
      A: ClassTag[A]): DataPipelineT[F, QueryResult[A, G, R], SparkStreaming] =
      `this`.mapRepr(trembitaqlForSparkStreaming.apply(_, queryF))
  }
  implicit class SparkStreamingFsmByKey[F[_], A](
      private val self: DataPipelineT[F, A, SparkStreaming]
  ) extends AnyVal {
    def fsmByKey[K: ClassTag, N, D, B: ClassTag](getKey: A => K)(
        initial: InitialState[N, D, F]
    )(fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B])(
        implicit sparkFSM: SparkStreamingFSM[F],
        A: ClassTag[A],
        F: SerializableMonad[F],
        run: Spark#Run[F]
    ): DataPipelineT[F, B, SparkStreaming] =
      self.mapRepr[B](sparkFSM.byKey[A, K, N, D, B](_)(getKey, initial)(fsmF))
  }

  implicit class SparkIOOps[A](val `this`: DataPipelineT[IO, A, SparkStreaming]) extends AnyVal with MagnetlessSparkStreamingIOOps[A]
  implicit class SparkFutureOps[A](private val `this`: DataPipelineT[Future, A, SparkStreaming]) extends AnyVal {
    def mapM[B: ClassTag](
        magnet: MagnetF[Future, A, B, SparkStreaming]
    )(implicit F: Monad[Future]): DataPipelineT[Future, B, SparkStreaming] =
      `this`.mapMImpl[A, B](magnet.prepared)
  }

  implicit def magnetFFromSpark[F[_], A, B](
      f: A => F[B]
  )(implicit ev: (A => F[B]) => MagnetF[F, A, B, Spark]): MagnetF[F, A, B, SparkStreaming] =
    new MagnetF[F, A, B, SparkStreaming] {
      override def prepared: A => F[B] = ev(f).prepared
    }

  case class GroupByImplicits[K, V](ctgK: ClassTag[K], ctgV: ClassTag[V], orderingK: Ordering[K])
  object GroupByImplicits {
    implicit def derive[K: ClassTag: Ordering, V: ClassTag]: GroupByImplicits[K, V] =
      GroupByImplicits[K, V](implicitly, implicitly, implicitly)
  }

  implicit val canGroupByOrderedDStream: CanGroupByOrdered[DStream] = new CanGroupByOrdered[DStream] {
    def groupBy[K: ClassTag: Ordering, V: ClassTag](fa: DStream[V])(f: V => K): DStream[(K, Iterable[V])] =
      fa.map(v => f(v) -> v).groupByKey()
  }

  implicit def liftIdToRdd(implicit ssc: StreamingContext): LiftPipeline[Id, SparkStreaming] = new LiftPipeline[Id, SparkStreaming] {
    override def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): DataPipelineT[Id, A, SparkStreaming] = DataPipelineT.fromRepr[Id, A, SparkStreaming] {
      val rdd   = ssc.sparkContext.parallelize(xs.toSeq)
      val queue = mutable.Queue(rdd)
      ssc.queueStream(queue)
    }
    override def liftIterableF[A: ClassTag](
        fa: Id[Iterable[A]]
    ): DataPipelineT[Id, A, SparkStreaming] = liftIterable(fa)
  }

  implicit def liftIOToRdd(implicit ssc: StreamingContext): LiftPipeline[IO, SparkStreaming] = new LiftPipeline[IO, SparkStreaming] {
    override def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): DataPipelineT[IO, A, SparkStreaming] = DataPipelineT.fromRepr[IO, A, SparkStreaming] {
      val rdd   = ssc.sparkContext.parallelize(xs.toSeq)
      val queue = mutable.Queue(rdd)
      ssc.queueStream(queue)
    }
    override def liftIterableF[A: ClassTag](
        fa: IO[Iterable[A]]
    ): DataPipelineT[IO, A, SparkStreaming] =
      DataPipelineT.fromReprF[IO, A, SparkStreaming](fa.map { xs =>
        val rdd   = ssc.sparkContext.parallelize(xs.toSeq)
        val queue = mutable.Queue(rdd)
        ssc.queueStream(queue)
      })
  }
}
