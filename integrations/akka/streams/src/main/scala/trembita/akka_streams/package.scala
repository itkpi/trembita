package trembita

import _root_.akka.NotUsed
import _root_.akka.stream._
import _root_.akka.stream.scaladsl._
import cats.effect.IO
import cats.{~>, Id, Monad}
import trembita.internal.EvaluatedSource
import trembita.operations._
import trembita.outputs.internal.OutputT
import scala.collection.parallel.immutable.ParVector
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

package object akka_streams extends operations {
  implicit class AkkaOps[F[_], A, Mat](
      val `this`: BiDataPipelineT[F, A, Akka[Mat]]
  ) extends AnyVal
      with MagnetlessOps[F, A, Akka[Mat]]
      with OutputDslAkka[F, A, Mat] {
    def through[B: ClassTag, Mat2](
        flow: Graph[FlowShape[A, B], Mat2]
    )(implicit E: Akka[Mat], run: Akka[Mat]#Run[F], F: Monad[F]): BiDataPipelineT[F, B, Akka[Mat2]] =
      EvaluatedSource.make[F, B, Akka[Mat2]](F.map(`this`.evalRepr)(_.viaMat(flow)(Keep.right)), F)
  }

  implicit def deriveAkka[Mat](
      implicit mat: Materializer,
      ec: ExecutionContext
  ): Akka[Mat] with Environment.ReprAux[Source[?, Mat]] =
    Akka.akka(ec, mat)

  implicit def liftToAkka[F[+ _]: Monad](
      implicit mat: Materializer,
      ec: ExecutionContext
  ): LiftPipeline[F, Akka[NotUsed]] = new LiftAkkaPipeline[F]

  implicit def deriveRunFutureOnAkka(implicit parallelism: Parallelism, mat: Materializer): RunAkka[Future] =
    new RunFutureOnAkka(parallelism)

  implicit def deriveRunIOOnAkka(implicit parallelism: Parallelism, mat: Materializer): RunAkka[IO] =
    new RunIOOnAkka(parallelism)

  implicit def parallelism[F[_], E <: Environment](
      p: Parallelism
  )(implicit RunDsl: RunDsl[F]): RunAkka[F] =
    RunDsl.toRunAkka(p)

  implicit class InputCompanionExtensions(private val self: Input.type) extends AnyVal {
    @inline def fromSource[A: ClassTag, Mat](source: Source[A, Mat]): DataPipeline[A, Akka[Mat]] = Input.repr[Akka[Mat]].create(source)
    @inline def fromSourceF[F[+ _]]                                                              = new fromSourceFDsl[F]()
  }

  class fromSourceFDsl[F[+ _]](val `dummy`: Boolean = true) extends AnyVal {
    def apply[A: ClassTag, Mat](sourceF: Source[A, Mat])(implicit F: Monad[F]): BiDataPipelineT[F, A, Akka[Mat]] =
      Input.reprF[F, Akka[Mat]].create(F.pure(sourceF))

    def empty[A: ClassTag](implicit F: Monad[F]): BiDataPipelineT[F, A, Akka[NotUsed]] =
      Input.reprF[F, Akka[NotUsed]].create(F.pure(Source.empty[A]))
  }

  implicit class OutputCompanionExtensions(private val self: Output.type) extends AnyVal {
    @inline def fromSink[A: ClassTag, Mat](
        sink: Sink[A, Mat]
    )(implicit materializer: Materializer): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], a) => Mat]] =
      new OutputT[Id, A, Akka[Mat]] {
        type Out[G[_], a] = Mat
        def apply(pipeline: BiDataPipelineT[Id, A, Akka[Mat]])(
            implicit F: Monad[Id],
            E: Akka[Mat],
            run: RunAkka[Id],
            A: ClassTag[A]
        ): Mat = pipeline.evalRepr.runWith(sink)
      }

    @inline def fromSinkF[F[_]] = new fromSinkFDsl[F]()
  }

  class fromSinkFDsl[F[_]](val `dummy`: Boolean = true) extends AnyVal {
    def apply[A: ClassTag, Mat0, Mat1](
        sink: Sink[A, Mat1]
    )(implicit materializer: Materializer): OutputT.Aux[F, A, Akka[Mat0], λ[(G[_], a) => F[Mat1]]] =
      new OutputT[F, A, Akka[Mat0]] {
        type Out[G[_], a] = F[Mat1]
        def apply(pipeline: BiDataPipelineT[F, A, Akka[Mat0]])(
            implicit F: Monad[F],
            E: Akka[Mat0],
            run: RunAkka[F],
            A: ClassTag[A]
        ): F[Mat1] = F.map(pipeline.evalRepr)(_.runWith(sink))
      }
  }

  implicit val seqToSource: InjectTaggedK[Vector, Source[?, NotUsed]] =
    InjectTaggedK.fromArrow[Vector, Source[?, NotUsed]](λ[Vector[?] ~> Source[?, NotUsed]](vec => Source(vec)))

  implicit val parToJStream: InjectTaggedK[ParVector, Source[?, NotUsed]] =
    InjectTaggedK.fromArrow[ParVector, Source[?, NotUsed]](λ[ParVector[?] ~> Source[?, NotUsed]](vec => Source(vec.seq)))
}
