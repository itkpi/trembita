package com.github.trembita.akka

import akka.NotUsed
import akka.stream.Materializer
import scala.language.{existentials, higherKinds}
import com.github.trembita._
import com.github.trembita.collections._
import akka.stream.scaladsl._
import cats.{~>, Functor, Monad}
import scala.annotation.implicitNotFound
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag
import cats.instances.future._

@implicitNotFound("""
    Operation you're performing requires implicit AkkaMat[${Mat}] in the scope.
    Please try to define implicit ExecutionContext and Materializer in the scope
  """)
sealed trait Akka[Mat] extends Environment {
  final type Repr[X]   = Source[X, Mat]
  final type Run[G[_]] = RunAkka[G]
  final type Result[X] = Future[X]

  implicit def executionContext: ExecutionContext
  implicit def materializer: Materializer

  val FlatMapRepr: ApplicativeFlatMap[Repr] =
    new ApplicativeFlatMap[Repr] {
      def map[A, B: ClassTag](fa: Source[A, Mat])(f: A => B): Source[B, Mat] =
        fa.map(f)

      def mapConcat[A, B: ClassTag](
          fa: Source[A, Mat]
      )(f: A => Iterable[B]): Source[B, Mat] =
        fa.mapConcat(f(_).toVector)
    }

  val FlatMapResult: Monad[Future] = Monad[Future]

  val TraverseRepr: TraverseTag[Repr, Run] = new TraverseTag[Repr, Run] {
    def traverse[G[_], A, B: ClassTag](
        fa: Source[A, Mat]
    )(f: A => G[B])(implicit G: Run[G]): G[Source[B, Mat]] =
      G.lift(G.traverse(fa)(f))
  }

  override def foreachF[F[_], A](
      repr: Source[A, Mat]
  )(f: A => F[Unit])(implicit Run: RunAkka[F], F: Functor[F]): F[Unit] =
    Run.traverse_(repr)(f)

  def collect[A, B: ClassTag](
      repr: Repr[A]
  )(pf: PartialFunction[A, B]): Repr[B] =
    repr.collect(pf)

  def distinctKeys[A: ClassTag, B: ClassTag](vs: Repr[(A, B)]): Repr[(A, B)] = {
    val distinctKeysFlow = Flow[(A, B)]
      .fold(Map.empty[A, B]) {
        case (m, (k, v)) => m + (k -> v)
      }
      .mapConcat { x =>
        x
      }

    vs.via(distinctKeysFlow)
  }

  def concat[A](xs: Repr[A], ys: Repr[A]): Repr[A] = xs ++ ys

  def zip[A, B: ClassTag](xs: Repr[A], ys: Repr[B]): Repr[(A, B)] =
    xs zip ys

  def memoize[A: ClassTag](xs: Repr[A]): Repr[A] =
    xs

  def foreach[A](repr: Repr[A])(f: A => Unit): Result[Unit] =
    repr.runForeach(f).map(_ => {})
}

object Akka {
  def akka[Mat](ec: ExecutionContext, mat: Materializer): Akka[Mat] =
    new {
      implicit val executionContext: ExecutionContext = ec
      implicit val materializer: Materializer         = mat
    } with Akka[Mat]
}
