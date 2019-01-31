package trembita.akka_streams

import akka.NotUsed
import akka.stream.scaladsl._
import akka.stream.Materializer
import trembita._
import trembita.operations.LiftPipeline
import scala.reflect.ClassTag
import cats.Monad
import cats.syntax.functor._
import scala.concurrent.ExecutionContext
import scala.language.higherKinds

class LiftAkkaPipeline[F[+ _]](implicit mat: Materializer, ec: ExecutionContext, F: Monad[F]) extends LiftPipeline[F, Akka[NotUsed]] {

  def liftIterable[A: ClassTag](xs: Iterable[A]): DataPipelineT[F, A, Akka[NotUsed]] =
    Input.fromSourceF[F](Source.fromIterator(() => xs.iterator))

  def liftIterableF[A: ClassTag](
      fa: F[Iterable[A]]
  ): DataPipelineT[F, A, Akka[NotUsed]] =
    Input
      .reprF[F, Akka[NotUsed]]
      .create(
        fa.map(xs => Source.fromIterator(() => xs.iterator))
      )
}
