package com.github.trembita

import cats.effect.IO
import cats.{~>, Eval, Id, Monad, StackSafeMonad}
import com.github.trembita.fsm.{FSM, InitialState}
import com.github.trembita.inputs.InputT
import com.github.trembita.internal.EvaluatedSource
import com.github.trembita.operations._
import com.github.trembita.ql.QueryBuilder.Query
import com.github.trembita.ql.{AggDecl, AggRes, GroupingCriteria, QueryBuilder, QueryResult}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}
import scala.util.control.NonFatal

package object spark extends LowPriorityInstancesForSpark {
  type SerializableFuture[+A] = SerializableFutureImpl.NewType[A]
  val SerializableFuture = SerializableFutureImpl

  implicit val futureToSerializableArrow: Future ~> SerializableFuture = λ[Future[?] ~> SerializableFuture[?]](SerializableFuture.widen(_))
  implicit val serializableFutureToIO: SerializableFuture ~> IO        = λ[SerializableFuture[?] ~> IO[?]](fa => IO.fromFuture(IO(fa)))

  implicit class SerializableFutureOps[A](private val self: SerializableFuture[A]) extends AnyVal {
    def bind[B](f: A => SerializableFuture[B]): SerializableFuture[B] =
      SerializableFuture.widen(self.flatMap(f))

    def handleErrorWith(
        f: Throwable => SerializableFuture[A]
    ): SerializableFuture[A] = SerializableFuture.widen(self.recoverWith { case t => f(t) })

    def handleError(
        f: Throwable => A
    ): SerializableFuture[A] = SerializableFuture.widen(self.recover { case t => f(t) })

    def attempt: SerializableFuture[Either[Throwable, A]] =
      SerializableFuture.widen(self.map(a => Right[Throwable, A](a)).recover {
        case NonFatal(t) => Left(t)
      })

    def where(p: A => Boolean): SerializableFuture[A] = SerializableFuture.widen(self.filter(p)(globalSafeEc))

    def fmap[B](f: A => B): SerializableFuture[B] = SerializableFuture.widen(self.map(f))
  }

  @transient implicit lazy val globalSafeEc: ExecutionContext = ExecutionContext.global

  @transient lazy implicit val safeFutureInstances
    : SerializableMonadError[SerializableFuture] with SerializableCoflatMap[SerializableFuture] with SerializableMonad[SerializableFuture] =
    new SerializableMonadError[SerializableFuture] with SerializableCoflatMap[SerializableFuture] with SerializableMonad[SerializableFuture]
    with StackSafeMonad[SerializableFuture] {
      def pure[A](x: A): SerializableFuture[A] = SerializableFuture.pure(x)

      def flatMap[A, B](fa: SerializableFuture[A])(f: A => SerializableFuture[B]): SerializableFuture[B] =
        fa.bind(f)

      def handleErrorWith[A](fea: SerializableFuture[A])(
          f: Throwable => SerializableFuture[A]
      ): SerializableFuture[A] = fea.handleErrorWith(f)

      def raiseError[A](e: Throwable): SerializableFuture[A] = SerializableFuture.raiseError(e)
      override def handleError[A](fea: SerializableFuture[A])(
          f: Throwable => A
      ): SerializableFuture[A] = fea.handleError(f)

      override def attempt[A](fa: SerializableFuture[A]): SerializableFuture[Either[Throwable, A]] =
        fa.attempt

      override def map[A, B](fa: SerializableFuture[A])(f: A => B): SerializableFuture[B] = fa.fmap(f)

      override def catchNonFatal[A](a: => A)(
          implicit ev: Throwable <:< Throwable
      ): SerializableFuture[A] = SerializableFuture.fromTry {
        try Success(a)
        catch {
          case NonFatal(e) => Failure(e)
        }
      }

      override def catchNonFatalEval[A](a: Eval[A])(
          implicit ev: Throwable <:< Throwable
      ): SerializableFuture[A] = catchNonFatal(a.value)

      def coflatMap[A, B](fa: SerializableFuture[A])(f: SerializableFuture[A] => B): SerializableFuture[B] =
        SerializableFuture.start(f(fa))
    }

  implicit val runIdOnSpark: RunOnSpark[cats.Id] = new RunIdOnSpark

  implicit def runFutureOnSpark(
      implicit timeout: AsyncTimeout
  ): RunOnSpark[Future] =
    new RunFutureOnSpark(timeout)

  implicit def runSerializableFutureOnSpark(
      implicit timeout: AsyncTimeout
  ): RunOnSpark[SerializableFuture] = runFutureOnSpark.asInstanceOf[RunOnSpark[SerializableFuture]]

  implicit def runIOOnSpark(implicit timeout: AsyncTimeout): RunOnSpark[IO] =
    new RunIOOnSpark(timeout)

  implicit class SparkOps[F[_], A](val `this`: DataPipelineT[F, A, Spark]) extends AnyVal with MagnetlessSparkBasicOps[F, A] {
    def query[G <: GroupingCriteria, T <: AggDecl, R <: AggRes, Comb](
        queryF: QueryBuilder.Empty[A] => Query[A, G, T, R, Comb]
    )(implicit trembitaqlForSpark: trembitaqlForSpark[A, G, T, R, Comb],
      run: Spark#Run[F],
      F: Monad[F],
      A: ClassTag[A]): DataPipelineT[F, QueryResult[A, G, R], Spark] =
      `this`.mapRepr(trembitaqlForSpark.apply(_, queryF))

    def mapM[B: ClassTag](magnet: MagnetF[F, A, B, Spark])(implicit F: Monad[F]): DataPipelineT[F, B, Spark] =
      `this`.mapMImpl[A, B](magnet.prepared)
  }

  implicit class SparkIOOps[A](val `this`: DataPipelineT[IO, A, Spark]) extends AnyVal with MagnetlessSparkIOOps[A]

  implicit def materializeFuture[A, B](
      f: A => SerializableFuture[B]
  ): MagnetF[SerializableFuture, A, B, BaseSpark] = macro rewrite.materializeFutureMagnet[A, B]

  implicit def turnVectorIntoRDD(
      implicit sc: SparkContext
  ): InjectTaggedK[Vector, RDD] = new InjectTaggedK[Vector, RDD] {
    def apply[A: ClassTag](fa: Vector[A]): RDD[A] = sc.parallelize(fa)
  }

  implicit def turnVectorIntoRDD2(
      implicit spark: SparkSession
  ): InjectTaggedK[Vector, RDD] = new InjectTaggedK[Vector, RDD] {
    def apply[A: ClassTag](fa: Vector[A]): RDD[A] = spark.sparkContext.parallelize(fa)
  }

  implicit val turnRDDIntoVector: InjectTaggedK[RDD, Vector] =
    InjectTaggedK.fromArrow[RDD, Vector](
      λ[RDD[?] ~> Vector[?]](_.collect().toVector)
    )

  implicit def turnParVectorIntoRDD(
      implicit sc: SparkContext
  ): InjectTaggedK[ParVector, RDD] = new InjectTaggedK[ParVector, RDD] {
    def apply[A: ClassTag](fa: ParVector[A]): RDD[A] = sc.parallelize(fa.seq)
  }

  implicit val turnRDDIntoParVector: InjectTaggedK[RDD, ParVector] =
    InjectTaggedK.fromArrow[RDD, ParVector](
      λ[RDD[?] ~> ParVector[?]](_.collect().toVector.par)
    )

  implicit val canSortRDD: CanSort[RDD] = new CanSort[RDD] {
    def sorted[A: Ordering: ClassTag](fa: RDD[A]): RDD[A] = fa.sortBy(identity)
    def sortedBy[A: ClassTag, B: Ordering: ClassTag](fa: RDD[A])(f: A => B): RDD[A] =
      fa.sortBy(f)
  }

  implicit class SparkFsmByKey[F[_], A](
      private val self: DataPipelineT[F, A, Spark]
  ) extends AnyVal {
    def fsmByKey[K: Encoder: ClassTag, N: Encoder, D: Encoder, B: ClassTag: TypeTag: Encoder](getKey: A => K)(
        initial: InitialState[N, D, F]
    )(fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B])(
        implicit sparkFSM: SparkFSM[F],
        A: ClassTag[A],
        AEnc: Encoder[A],
        F: SerializableMonad[F],
        run: Spark#Run[F]
    ): DataPipelineT[F, B, Spark] =
      self.mapRepr[B](sparkFSM.byKey[A, K, N, D, B](_)(getKey, initial)(fsmF))
  }

  implicit def runIODsl(timeout: AsyncTimeout): RunOnSpark[IO] =
    new RunIOOnSpark(timeout)

  implicit class DatasetOps[A](private val self: Dataset[A]) extends AnyVal {
    def filterIf(cond: Boolean)(p: Column): Dataset[A] =
      if (cond) self.filter(p)
      else self

    def filterMany(cond0: Column, rest: Column*): Dataset[A] =
      rest.foldLeft(self.filter(cond0))(_ filter _)
  }

  implicit val rddToVector: CanToVector.Aux[RDD, Id] = new CanToVector[RDD] {
    type Result[X] = X
    def apply[A](fa: RDD[A]): Vector[A] = fa.toLocalIterator.toVector
  }

  implicit val canGroupByRDD: CanGroupBy[RDD] = new CanGroupBy[RDD] {
    def groupBy[K: ClassTag, V: ClassTag](fa: RDD[V])(f: V => K): RDD[(K, Iterable[V])] = fa.groupBy(f)
  }

  implicit val canZipRDD: CanZip[RDD] = new CanZip[RDD] {
    def zip[A: ClassTag, B: ClassTag](
        fa: RDD[A],
        fb: RDD[B]
    ): RDD[(A, B)] = fa zip fb
  }

  implicit def liftIdToRdd(implicit sc: SparkContext): LiftPipeline[Id, Spark] = new LiftPipeline[Id, Spark] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): DataPipelineT[Id, A, Spark] = Input.repr[Spark].create(sc.parallelize(xs.toSeq))

    def liftIterableF[A: ClassTag](
        fa: Id[Iterable[A]]
    ): DataPipelineT[Id, A, Spark] = Input.repr[Spark].create(sc.parallelize(fa.toSeq))
  }

  implicit def liftIOToRdd(implicit sc: SparkContext): LiftPipeline[IO, Spark] = new LiftPipeline[IO, Spark] {
    def liftIterable[A: ClassTag](
        xs: Iterable[A]
    ): DataPipelineT[IO, A, Spark] = Input.reprF[IO, Spark].create(IO(sc.parallelize(xs.toSeq)))
    def liftIterableF[A: ClassTag](
        fa: IO[Iterable[A]]
    ): DataPipelineT[IO, A, Spark] = Input.reprF[IO, Spark].create(fa.map(xs => sc.parallelize(xs.toSeq)))
  }

  implicit def liftSerializableFutureToRdd(implicit sc: SparkContext): LiftPipeline[SerializableFuture, Spark] =
    new LiftPipeline[SerializableFuture, Spark] {
      def liftIterable[A: ClassTag](
          xs: Iterable[A]
      ): DataPipelineT[SerializableFuture, A, Spark] = Input.rddF[SerializableFuture].create(sc.parallelize(xs.toSeq))
      def liftIterableF[A: ClassTag](
          fa: SerializableFuture[Iterable[A]]
      ): DataPipelineT[SerializableFuture, A, Spark] =
        Input.reprF[SerializableFuture, Spark].create(fa.fmap[RDD[A]](xs => sc.parallelize(xs.toSeq)))
    }

  implicit class InputCompanionExtension(private val self: Input.type) extends AnyVal {
    @inline def rdd: InputT[Id, Spark, RDD] = new InputT[Id, Spark, RDD] {
      def create[A: ClassTag](props: RDD[A])(
          implicit F: Monad[Id]
      ): DataPipelineT[Id, A, Spark] = EvaluatedSource.make[Id, A, Spark](safeIdInstances.pure(props), safeIdInstances)
    }

    @inline def rddF[F[_]](implicit sr: SerializableMonad[F]): InputT[F, Spark, RDD] = new InputT[F, Spark, RDD] {
      def create[A: ClassTag](props: RDD[A])(
          implicit F: Monad[F]
      ): DataPipelineT[F, A, Spark] = EvaluatedSource.make[F, A, Spark](sr.pure(props), sr)
    }
  }

  class ArrayOutputDSL(val `dummy`: Boolean = true) extends AnyVal {
    def apply[F[_], A] = new ArrayOutput[F, A]
  }

  trait LowPriorityArrayOutputDsl {
    implicit def toOutputF[F[_], A](dsl: ArrayOutputDSL): ArrayOutput[F, A] = dsl[F, A]
  }

  object ArrayOutputDSL extends LowPriorityArrayOutputDsl {
    implicit def toOutputId[A](dsl: ArrayOutputDSL): ArrayOutput[Id, A] = dsl[Id, A]
  }

  implicit class OutputCompanionSparkExtensions(private val self: Output.type) extends AnyVal {
    @inline def array = new ArrayOutputDSL
  }
}
