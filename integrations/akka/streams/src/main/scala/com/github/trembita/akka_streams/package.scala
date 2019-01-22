package com.github.trembita

import _root_.akka.NotUsed
import _root_.akka.stream._
import _root_.akka.stream.scaladsl._
import cats.effect.{Effect, IO, Timer}
import cats.{Id, Monad}
import com.github.trembita.fsm.{CanFSM, FSM, InitialState}
import com.github.trembita.internal.EvaluatedSource
import com.github.trembita.operations._
import com.github.trembita.outputs.internal.{outputWithoutPropsDsl, OutputDsl, OutputT}

import scala.collection.immutable.SortedMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{ExecutionContext, Future}
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

package object akka_streams {
  implicit class AkkaOps[F[_], A, Mat](
      val `this`: DataPipelineT[F, A, Akka[Mat]]
  ) extends AnyVal
      with MagnetlessOps[F, A, Akka[Mat]]
      with OutputDslAkka[F, A, Mat] {
    def through[B: ClassTag, Mat2](
        flow: Graph[FlowShape[A, B], Mat2]
    )(implicit E: Akka[Mat], run: Akka[Mat]#Run[F], F: Monad[F]): DataPipelineT[F, B, Akka[Mat2]] =
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

  implicit def akkaCanFSM[F[_], Mat](
      implicit akkaFSM: AkkaFSM[F, Mat],
      F: Monad[F],
      E: Akka[Mat],
      run: Akka[Mat]#Run[F]
  ): CanFSM[F, Akka[Mat]] =
    new CanFSM[F, Akka[Mat]] {
      def fsm[A: ClassTag, N, D, B: ClassTag](
          pipeline: DataPipelineT[F, A, Akka[Mat]]
      )(initial: InitialState[N, D, F])(
          fsmF: FSM.Empty[F, N, D, A, B] => FSM.Func[F, N, D, A, B]
      ): DataPipelineT[F, B, Akka[Mat]] =
        pipeline.mapRepr[B](akkaFSM[A, N, D, B](_)(initial)(fsmF))
    }

  implicit def canFlatMapAkka[Mat]: CanFlatMap[Akka[Mat]] = new CanFlatMap[Akka[Mat]] {
    override def flatMap[A, B](fa: Source[A, Mat])(
        f: A => Source[B, Mat]
    ): Source[B, Mat] = fa.flatMapConcat(f)
  }

  implicit def canGroupByAkka[Mat]: CanGroupBy[Source[?, Mat]] = new CanGroupBy[Source[?, Mat]] {
    import com.github.trembita.collections._

    def groupBy[K: ClassTag, V: ClassTag](vs: Source[V, Mat])(f: V => K): Source[(K, Iterable[V]), Mat] = {
      val groupFlow: Flow[(K, V), (K, Iterable[V]), NotUsed] = Flow[(K, V)]
        .fold(Map.empty[K, Vector[V]]) {
          case (m, (k, v)) => m.modify(k, Vector(v))(_ :+ v)
        }
        .mapConcat { x =>
          x
        }

      vs.map(v => f(v) -> v).via(groupFlow)
    }
  }

  implicit def canSpanByAkka[Mat]: CanSpanBy[Source[?, Mat]] = new CanSpanBy[Source[?, Mat]] {
    override def spanBy[K: ClassTag, V: ClassTag](fa: Source[V, Mat])(
        f: V => K
    ): Source[(K, Iterable[V]), Mat] =
      fa.map(v => f(v) -> v)
        .via(new SpanByFlow[K, V, ListBuffer[V]](v => ListBuffer(v), _ :+ _))
        .map { case (k, vs) => (k, vs.toVector) }
  }

  implicit def canGroupByOrderedAkka[Mat]: CanGroupByOrdered[Source[?, Mat]] = new CanGroupByOrdered[Source[?, Mat]] {
    import com.github.trembita.collections._

    def groupBy[K: ClassTag: Ordering, V: ClassTag](vs: Source[V, Mat])(f: V => K): Source[(K, Iterable[V]), Mat] = {
      val groupFlow: Flow[(K, V), (K, Iterable[V]), NotUsed] = Flow[(K, V)]
        .fold(SortedMap.empty[K, Vector[V]]) {
          case (m, (k, v)) => m.modify(k, Vector(v))(_ :+ v)
        }
        .mapConcat { x =>
          x
        }

      vs.map(v => f(v) -> v).via(groupFlow)
    }
  }

  implicit def canZipAkka[Mat]: CanZip[Source[?, Mat]] = new CanZip[Source[?, Mat]] {
    def zip[A: ClassTag, B: ClassTag](fa: Source[A, Mat], fb: Source[B, Mat]): Source[(A, B), Mat] = fa zip fb
  }

  implicit def canPauseAkka[F[_]: Effect: Timer, Mat](
      implicit mat: ActorMaterializer,
      ec: ExecutionContext,
      runAkka: RunAkka[F]
  ): CanPause[F, Akka[Mat]] = new CanPauseAkkaF[F, Mat]

  implicit def canPause2Akka[F[_]: Effect: Timer, Mat](
      implicit mat: ActorMaterializer,
      ec: ExecutionContext,
      runAkka: RunAkka[F]
  ): CanPause2[F, Akka[Mat]] = new CanPause2AkkaF[F, Mat]

  implicit def akkaCanToVector[Mat](implicit mat: ActorMaterializer): CanToVector.Aux[Source[?, Mat], Future] =
    new CanToVector[Source[?, Mat]] {
      override type Result[X] = Future[X]
      override def apply[A](fa: Source[A, Mat]): Future[Vector[A]] = fa.runWith(Sink.collection[A, Vector[A]])
    }

  implicit class InputCompanionExtensions(private val self: Input.type) extends AnyVal {
    def fromSource[A: ClassTag, Mat](source: Source[A, Mat]): DataPipeline[A, Akka[Mat]] = Input.repr[Akka[Mat]].create(source)
    def fromSourceF[F[+ _], A: ClassTag, Mat](sourceF: F[Source[A, Mat]])(implicit F: Monad[F]): DataPipelineT[F, A, Akka[Mat]] =
      Input.reprF[F, Akka[Mat]].create(sourceF)
  }

  implicit class OutputCompanionExtensions(private val self: Output.type) extends AnyVal {
    def fromSink[A: ClassTag, Mat](
        sink: Sink[A, Mat]
    )(implicit materializer: Materializer): OutputT.Aux[Id, A, Akka[Mat], λ[(G[_], a) => Mat]] =
      new OutputT[Id, A, Akka[Mat]] {
        type Out[G[_], a] = Mat
        def apply(pipeline: DataPipelineT[Id, A, Akka[Mat]])(
            implicit F: Monad[Id],
            E: Akka[Mat],
            run: RunAkka[Id],
            A: ClassTag[A]
        ): Mat = pipeline.evalRepr.runWith(sink)
      }

    def fromSinkF[F[_], A: ClassTag, Mat0, Mat1](
        sink: Sink[A, Mat1]
    )(implicit materializer: Materializer): OutputT.Aux[F, A, Akka[Mat0], λ[(G[_], a) => F[Mat1]]] =
      new OutputT[F, A, Akka[Mat0]] {
        type Out[G[_], a] = F[Mat1]
        def apply(pipeline: DataPipelineT[F, A, Akka[Mat0]])(
            implicit F: Monad[F],
            E: Akka[Mat0],
            run: RunAkka[F],
            A: ClassTag[A]
        ): F[Mat1] = F.map(pipeline.evalRepr)(_.runWith(sink))
      }
  }
}
