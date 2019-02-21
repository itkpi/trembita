import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.data.Kleisli
import trembita.internal.{BiMapPipelineT => _, _}
import trembita.operations._
import trembita.outputs.internal.{lowPriorityTricks, OutputDsl}
import scala.reflect.ClassTag

package object trembita extends standardMagnets with arrows with lowPriorityTricks {

  type DataPipelineT[F[_], A, E <: Environment] = BiDataPipelineT[F, Throwable, A, E]
  type DataPipeline[A, E <: Environment]        = BiDataPipelineT[Id, Nothing, A, E]

  type BiMapPipelineT[F[_], Er, K, V, E <: Environment] = internal.BiMapPipelineT[F, Er, K, V, E]
  type MapPipelineT[F[_], K, V, E <: Environment]       = internal.BiMapPipelineT[F, Throwable, K, V, E]
  type MapPipeline[K, V, E <: Environment]              = internal.BiMapPipelineT[Id, Nothing, K, V, E]

  type BiPairPipelineT[F[_], Er, K, V, Ex <: Environment] = BiDataPipelineT[F, Er, (K, V), Ex]
  type PairPipelineT[F[_], K, V, Ex <: Environment]       = BiDataPipelineT[F, Throwable, (K, V), Ex]
  type PairPipeline[K, V, Ex <: Environment]              = BiDataPipelineT[Id, Nothing, (K, V), Ex]

  type Supports[E <: Environment, Op[_[_]]] = Op[E#Repr]
  type Run[F[_], E <: Environment]          = E#Run[F]

  implicit class CommonOps[F[_], Er, A, E <: Environment](
      val `this`: BiDataPipelineT[F, Er, A, E]
  ) extends AnyVal
      with EnvironmentIndependentOps[F, Er, A, E]
      with EnvironmentDependentOps[F, Er, A, E]
      with OutputDsl[F, Er, A, E]

  implicit class SeqOps[F[_], Er, A](val `this`: BiDataPipelineT[F, Er, A, Sequential])
      extends AnyVal
      with MagnetlessOps[F, Er, A, Sequential]

  implicit class ParOps[F[_], Er, A](val `this`: BiDataPipelineT[F, Er, A, Parallel]) extends AnyVal with MagnetlessOps[F, Er, A, Parallel]

  /**
    * Operations for [[BiDataPipelineT]] of tuples
    * (NOT [[BiMapPipelineT]])
    **/
  implicit class PairPipelineOps[F[_], Er, K, V, E <: Environment](
      val self: BiPairPipelineT[F, Er, K, V, E]
  ) extends AnyVal {
    def mapValues[W](
        f: V => W
    )(implicit F: Monad[F]): BiPairPipelineT[F, Er, K, W, E] = self.mapImpl {
      case (k, v) => (k, f(v))
    }

    def keys(implicit F: Monad[F], K: ClassTag[K]): BiDataPipelineT[F, Er, K, E] =
      self.mapImpl(_._1)

    def values(implicit F: Monad[F], V: ClassTag[V]): BiDataPipelineT[F, Er, V, E] =
      self.mapImpl(_._2)

    /** @return - [[BiMapPipelineT]] */
    def toMapPipeline(implicit K: ClassTag[K], V: ClassTag[V], F: Monad[F], Er: ClassTag[Er]): BiMapPipelineT[F, Er, K, V, E] =
      new BaseMapPipelineT[F, Er, K, V, E](
        self.asInstanceOf[BiDataPipelineT[F, Er, (K, V), E]],
        F
      )

    def reduceByKey(f: (V, V) => V)(
        implicit canReduceByKey: CanReduceByKey[E#Repr],
        F: MonadError[F, Er],
        E: E,
        run: E#Run[F],
        K: ClassTag[K],
        V: ClassTag[V]
    ): BiDataPipelineT[F, Er, (K, V), E] = self.mapRepr { repr =>
      canReduceByKey.reduceByKey(repr)(f)
    }

    def combineByKey[C: ClassTag](
        init: V => C,
        addValue: (C, V) => C,
        mergeCombiners: (C, C) => C
    )(
        implicit canCombineByKey: CanCombineByKey[E#Repr],
        F: MonadError[F, Er],
        E: E,
        run: E#Run[F],
        K: ClassTag[K],
        V: ClassTag[V]
    ): BiDataPipelineT[F, Er, (K, C), E] = self.mapRepr { repr =>
      canCombineByKey.combineByKey(repr)(init, addValue, mergeCombiners)
    }

    def combineByKey[C: ClassTag](parallelism: Int)(
        init: V => C,
        addValue: (C, V) => C,
        mergeCombiners: (C, C) => C
    )(
        implicit canCombineByKey: CanCombineByKeyWithParallelism[E#Repr],
        F: MonadError[F, Er],
        E: E,
        run: E#Run[F],
        K: ClassTag[K],
        V: ClassTag[V]
    ): BiDataPipelineT[F, Er, (K, C), E] = self.mapRepr { repr =>
      canCombineByKey.combineByKey(repr, parallelism)(init, addValue, mergeCombiners)
    }
  }

  type BiPipeT[F[_], Er, A, B, E <: Environment] = Kleisli[BiDataPipelineT[F, Er, ?, E], BiDataPipelineT[F, Er, A, E], B]
  type PipeT[F[_], A, B, E <: Environment]       = BiPipeT[F, Throwable, A, B, E]
  type Pipe[A, B, E <: Environment]              = BiPipeT[Id, Nothing, A, B, E]

  @inline def biPipeT[F[_], Er, A, B, E <: Environment](
      f: BiDataPipelineT[F, Er, A, E] => BiDataPipelineT[F, Er, B, E]
  ): BiPipeT[F, Er, A, B, E] =
    Kleisli[BiDataPipelineT[F, Er, ?, E], BiDataPipelineT[F, Er, A, E], B](f)

  @inline def pipeT[F[_], A, B, E <: Environment](f: DataPipelineT[F, A, E] => DataPipelineT[F, B, E]): PipeT[F, A, B, E] =
    Kleisli[DataPipelineT[F, ?, E], DataPipelineT[F, A, E], B](f)

  @inline def pipe[A, B, E <: Environment](f: DataPipeline[A, E] => DataPipeline[B, E]): Pipe[A, B, E] =
    biPipeT[Id, Nothing, A, B, E](f)

  type Sequential = Environment.Sequential
  type Parallel   = Environment.Parallel

  type FileInput   = inputs.FileInput
  type RandomInput = inputs.RandomInput
  type RepeatInput = inputs.RepeatInput

  val FileInput   = inputs.FileInput
  val RandomInput = inputs.RandomInput
  val RepeatInput = inputs.RepeatInput
}
