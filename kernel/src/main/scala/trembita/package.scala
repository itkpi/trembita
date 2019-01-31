import scala.language.{higherKinds, implicitConversions}
import cats._
import cats.data.Kleisli
import trembita.internal.{MapPipelineT => _, _}
import trembita.operations._
import trembita.outputs.internal.{lowPriorityTricks, OutputDsl}
import scala.reflect.ClassTag

package object trembita extends standardMagnets with arrows with lowPriorityTricks {

  type DataPipeline[A, E <: Environment]            = DataPipelineT[Id, A, E]
  type MapPipelineT[F[_], K, V, E <: Environment]   = internal.MapPipelineT[F, K, V, E]
  type PairPipelineT[F[_], K, V, Ex <: Environment] = DataPipelineT[F, (K, V), Ex]
  type Supports[E <: Environment, Op[_[_]]]         = Op[E#Repr]
  type Run[F[_], E <: Environment]                  = E#Run[F]

  implicit class CommonOps[F[_], A, E <: Environment](
      val `this`: DataPipelineT[F, A, E]
  ) extends AnyVal
      with EnvironmentIndependentOps[F, A, E]
      with EnvironmentDependentOps[F, A, E]
      with OutputDsl[F, A, E]

  implicit class SeqOps[F[_], A](val `this`: DataPipelineT[F, A, Sequential]) extends AnyVal with MagnetlessOps[F, A, Sequential]

  implicit class ParOps[F[_], A](val `this`: DataPipelineT[F, A, Parallel]) extends AnyVal with MagnetlessOps[F, A, Parallel]

  /**
    * Operations for [[DataPipelineT]] of tuples
    * (NOT [[MapPipelineT]])
    **/
  implicit class PairPipelineOps[F[_], K, V, E <: Environment](
      val self: PairPipelineT[F, K, V, E]
  ) extends AnyVal {
    def mapValues[W](
        f: V => W
    )(implicit F: Monad[F]): PairPipelineT[F, K, W, E] = self.mapImpl {
      case (k, v) => (k, f(v))
    }

    def keys(implicit F: Monad[F], K: ClassTag[K]): DataPipelineT[F, K, E] =
      self.mapImpl(_._1)

    def values(implicit F: Monad[F], V: ClassTag[V]): DataPipelineT[F, V, E] =
      self.mapImpl(_._2)

    /** @return - [[MapPipelineT]] */
    def toMapPipeline(implicit K: ClassTag[K], V: ClassTag[V], F: Monad[F]): MapPipelineT[F, K, V, E] =
      new BaseMapPipelineT[F, K, V, E](
        self.asInstanceOf[DataPipelineT[F, (K, V), E]],
        F
      )

    def reduceByKey(f: (V, V) => V)(
        implicit canReduceByKey: CanReduceByKey[E#Repr],
        F: Monad[F],
        E: E,
        run: E#Run[F],
        K: ClassTag[K],
        V: ClassTag[V]
    ): DataPipelineT[F, (K, V), E] = self.mapRepr { repr =>
      canReduceByKey.reduceByKey(repr)(f)
    }

    def combineByKey[C: ClassTag](
        init: V => C,
        addValue: (C, V) => C,
        mergeCombiners: (C, C) => C
    )(
        implicit canCombineByKey: CanCombineByKey[E#Repr],
        F: Monad[F],
        E: E,
        run: E#Run[F],
        K: ClassTag[K],
        V: ClassTag[V]
    ): DataPipelineT[F, (K, C), E] = self.mapRepr { repr =>
      canCombineByKey.combineByKey(repr)(init, addValue, mergeCombiners)
    }

    def combineByKey[C: ClassTag](parallelism: Int)(
        init: V => C,
        addValue: (C, V) => C,
        mergeCombiners: (C, C) => C
    )(
        implicit canCombineByKey: CanCombineByKeyWithParallelism[E#Repr],
        F: Monad[F],
        E: E,
        run: E#Run[F],
        K: ClassTag[K],
        V: ClassTag[V]
    ): DataPipelineT[F, (K, C), E] = self.mapRepr { repr =>
      canCombineByKey.combineByKey(repr, parallelism)(init, addValue, mergeCombiners)
    }
  }

  type PipeT[F[_], A, B, E <: Environment] = Kleisli[DataPipelineT[F, ?, E], DataPipelineT[F, A, E], B]
  type Pipe[A, B, E <: Environment]        = PipeT[Id, A, B, E]

  @inline def pipeT[F[_], A, B, E <: Environment](f: DataPipelineT[F, A, E] => DataPipelineT[F, B, E]): PipeT[F, A, B, E] =
    Kleisli[DataPipelineT[F, ?, E], DataPipelineT[F, A, E], B](f)

  @inline def pipe[A, B, E <: Environment](f: DataPipeline[A, E] => DataPipeline[B, E]): Pipe[A, B, E] =
    pipeT[Id, A, B, E](f)

  type Create[F[_], A, E <: Environment] = () => DataPipelineT[F, A, E]
  type Sequential                        = Environment.Sequential
  type Parallel                          = Environment.Parallel

  type FileInput   = inputs.FileInput
  type RandomInput = inputs.RandomInput
  type RepeatInput = inputs.RepeatInput

  val FileInput   = inputs.FileInput
  val RandomInput = inputs.RandomInput
  val RepeatInput = inputs.RepeatInput
}
