package trembita

import java.util.function.{BiConsumer, Supplier}
import trembita.operations.MagnetlessOps
import java.util.stream._
import cats.{Id, Monad}
import trembita.outputs.internal._
import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._

package object jstreams extends operations {
  implicit class JStreamsOps[F[_], A, T <: StreamType](val `this`: DataPipelineT[F, A, JavaStreams[T]])
      extends AnyVal
      with MagnetlessOps[F, A, JavaStreams[T]]

  class collectDsl(val `dummy`: Boolean = true) extends AnyVal {
    @inline def into[F[_], ST <: StreamType, A, Acc, R](
        collector: Collector[_ >: R, Acc, R]
    ): OutputT.Aux[F, R, JavaStreams[ST], λ[(G[_], a) => G[R]]] =
      new OutputT[F, R, JavaStreams[ST]] {
        type Out[G[_], a] = G[R]

        def apply(pipeline: DataPipelineT[F, R, JavaStreams[ST]])(
            implicit F: Monad[F],
            E: JavaStreams[ST],
            run: Monad[F],
            A: ClassTag[R]
        ): F[R] = F.map(pipeline.evalRepr(E, run))(stream => stream.collect[R, Acc](collector))
      }

  }

  implicit class JStreamsOutputOps(private val self: Output.type) extends AnyVal {
    @inline def collect: collectDsl = new collectDsl()
  }

  implicit def dslToSeqOutput[F[_], Col[x] <: Iterable[x], ST <: StreamType](
      dsl: collectionDsl[Col]
  ): OutputWithPropsT.Aux[F, JavaStreams[ST], λ[A => CanBuildFrom[Col[A], A, Col[A]]], λ[(G[_], A) => G[Col[A]]]] =
    new CollectionOutput[Col, F, JavaStreams[ST]] {
      protected def intoCollection[A: ClassTag](
          repr: Stream[A]
      )(implicit F: Monad[F], cbf: CanBuildFrom[Col[A], A, Col[A]]): F[Col[A]] = F.map(F.pure(repr)) { repr =>
        val builderRes: mutable.Builder[A, Col[A]] = repr.collect(
          (() => cbf()).asJava,
          new BiConsumer[mutable.Builder[A, Col[A]], A] {
            def accept(t: mutable.Builder[A, Col[A]], u: A): Unit = t += u
          },
          new BiConsumer[mutable.Builder[A, Col[A]], mutable.Builder[A, Col[A]]] {
            def accept(t: mutable.Builder[A, Col[A]], u: mutable.Builder[A, Col[A]]): Unit = t ++= u.result()
          }
        )
        builderRes.result()
      }
    }

  class jstreamInputDslF[F[+ _], ST <: StreamType](val `dummy`: Boolean = true) {
    @inline def create[A: ClassTag](values: F[Seq[A]])(implicit F: Monad[F],
                                                       creator: StreamCreator[ST]): DataPipelineT[F, A, JavaStreams[ST]] =
      Input.sequentialF[F, Seq].create(values).to[JavaStreams[ST]]

    @inline def empty[A: ClassTag](implicit F: Monad[F], creator: StreamCreator[ST]): DataPipelineT[F, A, JavaStreams[ST]] =
      create(F.pure(Seq.empty[A]))
  }

  class jstreamInputDsl[ST <: StreamType](val `dummy`: Boolean = true) {
    @inline def create[A: ClassTag](values: Seq[A])(implicit F: Monad[Id], creator: StreamCreator[ST]): DataPipeline[A, JavaStreams[ST]] =
      Input.sequential[Seq].create(values).to[JavaStreams[ST]]

    @inline def empty[A: ClassTag](implicit F: Monad[Id], creator: StreamCreator[ST]): DataPipeline[A, JavaStreams[ST]] =
      create(Seq.empty[A])
  }

  implicit class JStreamsInputOps(private val self: Input.type) extends AnyVal {
    @inline def jstream[ST <: StreamType]          = new jstreamInputDsl[ST]()
    @inline def jstreamF[F[+ _], ST <: StreamType] = new jstreamInputDslF[F, ST]()
  }
}
