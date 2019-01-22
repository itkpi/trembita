package com.github.trembita.spark

import cats.effect.{IO, Sync}
import cats.{Id, Monad}
import com.github.trembita._
import com.github.trembita.fsm.{FSM, InitialState}
import com.github.trembita.inputs.InputT
import com.github.trembita.internal.EvaluatedSource
import com.github.trembita.operations.{CanGroupByOrdered, LiftPipeline, MagnetF}
import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql.{AggDecl, AggRes, GroupingCriteria, QueryBuilder, QueryResult}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.language.{higherKinds, implicitConversions}

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

    def fsmByKey[K: ClassTag, N, D, B: ClassTag](getKey: A => K)(
        initial: InitialState[N, D, F]
    )(fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B])(
        implicit sparkFSM: SparkStreamingFSM[F],
        A: ClassTag[A],
        F: SerializableMonad[F],
        run: Spark#Run[F]
    ): DataPipelineT[F, B, SparkStreaming] =
      `this`.mapRepr[B](sparkFSM.byKey[A, K, N, D, B](_)(getKey, initial)(fsmF))

    def mapM[B: ClassTag](
        magnet: MagnetF[F, A, B, SparkStreaming]
    )(implicit F: SerializableMonad[F]): DataPipelineT[F, B, SparkStreaming] =
      `this`.mapMImpl[A, B](magnet.prepared)

  }

  implicit class SparkIOOps[A](val `this`: DataPipelineT[IO, A, SparkStreaming]) extends AnyVal with MagnetlessSparkStreamingIOOps[A]

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
    ): DataPipelineT[Id, A, SparkStreaming] = Input.repr[SparkStreaming].create[A] {
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
    ): DataPipelineT[IO, A, SparkStreaming] =
      Input
        .reprF[IO, SparkStreaming]
        .create[A](IO {
          val rdd   = ssc.sparkContext.parallelize(xs.toSeq)
          val queue = mutable.Queue(rdd)
          ssc.queueStream(queue)
        })

    override def liftIterableF[A: ClassTag](
        fa: IO[Iterable[A]]
    ): DataPipelineT[IO, A, SparkStreaming] =
      Input
        .reprF[IO, SparkStreaming]
        .create(fa.map { xs =>
          val rdd   = ssc.sparkContext.parallelize(xs.toSeq)
          val queue = mutable.Queue(rdd)
          ssc.queueStream(queue)
        })
  }

  implicit class InputCompanionExtensions(private val self: Input.type) extends AnyVal {
    @inline def dstream: InputT[Id, SparkStreaming, DStream] = new InputT[Id, SparkStreaming, DStream] {
      def create[A: ClassTag](props: DStream[A])(
          implicit F: Monad[Id]
      ): DataPipelineT[Id, A, SparkStreaming] = EvaluatedSource.make[Id, A, SparkStreaming](safeIdInstances.pure(props), safeIdInstances)
    }

    @inline def rddF[F[_]](implicit sr: SerializableMonad[F]): InputT[F, SparkStreaming, DStream] = new InputT[F, SparkStreaming, DStream] {
      def create[A: ClassTag](props: DStream[A])(
          implicit F: Monad[F]
      ): DataPipelineT[F, A, SparkStreaming] = EvaluatedSource.make[F, A, SparkStreaming](sr.pure(props), sr)
    }
  }

  class StartDsl(val `dummy`: Boolean = true) extends AnyVal {
    def apply[F[+ _], A](sync: (() => Unit) => F[Unit]) = new StartOutput[F, A](sync)
  }

  trait LowPriorityStartDsl {
    implicit def toOutput[F[+ _], A](dsl: StartDsl)(implicit F: Sync[F]): StartOutput[F, A] =
      new StartOutput[F, A](thunk => F.delay(thunk()))
  }

  object StartDsl extends LowPriorityStartDsl {
    implicit def toOutputId[A](dsl: StartDsl): StartOutput[Id, A] =
      new StartOutput[Id, A](thunk => thunk())
  }

  implicit class OutputCompanionSparkStreamingExtensions(val `this`: Output.type) extends AnyVal {
    @inline def start = new StartDsl
  }
}
